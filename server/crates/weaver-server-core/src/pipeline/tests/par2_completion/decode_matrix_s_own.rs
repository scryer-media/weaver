//! The decode matrix's own budget
//! What the quick pass is allowed to be blocked by
//! The whole-file-CRC quick-verification arm.
//! Evidence seeding on the authoritative pass.
//! Repairing on the verdict that asked for the recovery.

use super::*;

// ---------------------------------------------------------------------------
// The decode matrix's own budget
// ---------------------------------------------------------------------------

/// The budget is what refuses this set, and raising it is what accepts it.
///
/// The differential is the whole test: the same bytes, the same damage, the
/// same recovery — only the limit moves. That is what makes the refusal
/// actionable rather than a dead end, and it also settles what weaver's own
/// 64 MiB default has to do with it: nothing. The default is far below the
/// budget's floor, so the floor is what ruled, and only an explicitly larger
/// limit changes the answer.
#[test]
fn the_decode_matrix_budget_refuses_only_absurdly_damaged_sets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let payload = matrix_budget_payload();
    let par2_set = build_par2_set_with_uncomputed_recovery(
        MATRIX_BUDGET_FILENAME,
        &payload,
        MATRIX_BUDGET_SLICE_SIZE,
        MATRIX_BUDGET_MISSING_SLICES,
    );
    std::fs::write(
        temp_dir.path().join(MATRIX_BUDGET_FILENAME),
        matrix_budget_damaged_payload(&payload),
    )
    .unwrap();

    // What weaver actually configures.
    let refused = analyze_with_memory_limit(
        temp_dir.path(),
        &par2_set,
        crate::pipeline::completion::finalize::check::configured_par2_repair_memory_limit_bytes(),
    );
    assert_eq!(
        refused.verification.total_missing_blocks as usize, MATRIX_BUDGET_MISSING_SLICES,
        "the fixture must present exactly the damage it punched out"
    );
    match &refused.verification.repairable {
        par2_rs::verify::Repairability::ResourceLimited { reason } => assert!(
            reason.contains("matrix workspace budget"),
            "the refusal must be the workspace budget, not a format cap: {reason}"
        ),
        other => panic!("expected a resource-limited verdict, got {other:?}"),
    }

    // The same set with room for the workspace.
    let allowed = analyze_with_memory_limit(temp_dir.path(), &par2_set, 2 << 30);
    match &allowed.verification.repairable {
        par2_rs::verify::Repairability::Repairable { blocks_needed, .. } => assert_eq!(
            *blocks_needed as usize, MATRIX_BUDGET_MISSING_SLICES,
            "raising the limit must leave the damage untouched"
        ),
        other => panic!("expected a repairable verdict once the budget allows it, got {other:?}"),
    }
}

/// A job refused for a resource limit must say which knob exists.
///
/// The refusal used to reach the operator as a bare internal message with no
/// stated remedy, which is how a *tunable* limit ends up looking permanent.
#[tokio::test]
async fn a_resource_limited_par2_verdict_names_the_memory_override() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30470);
    let payload = matrix_budget_payload();
    let spec = standalone_job_spec(
        "Silver Horizon Beyond The Matrix Budget",
        &[(MATRIX_BUDGET_FILENAME.to_string(), payload.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_par2_set_with_uncomputed_recovery(
            MATRIX_BUDGET_FILENAME,
            &payload,
            MATRIX_BUDGET_SLICE_SIZE,
            MATRIX_BUDGET_MISSING_SLICES,
        ),
        &[],
    );
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        MATRIX_BUDGET_FILENAME,
        &matrix_budget_damaged_payload(&payload),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    match job_status_for_assert(&pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => {
            assert!(
                error.contains("WEAVER_PAR2_REPAIR_MEMORY_LIMIT_BYTES"),
                "a resource-limited failure must name the knob that raises the budget: {error}"
            );
            assert!(
                error.contains("matrix workspace budget"),
                "and must carry the verdict's own reason: {error}"
            );
        }
        other => panic!(
            "expected a resource-limited failure, got {other:?}; {}",
            debug_job_state(&pipeline, job_id)
        ),
    }
}

// ---------------------------------------------------------------------------
// What the quick pass is allowed to be blocked by
// ---------------------------------------------------------------------------

/// A clean payload beside a short *unprotected* file must not force a
/// whole-set read.
///
/// The quick pass answers for the recovery set, so the only thing that can stop
/// it is a file the set describes. An `.nfo` that lost an article is not one:
/// the pass could not have spoken for it either way, and letting it turn the
/// job away meant reading every byte of a payload already proven — the cost
/// that made a clean job look like a slow one.
#[tokio::test]
async fn a_short_unprotected_file_does_not_force_the_authoritative_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30471);
    let payload_filename = "silver.horizon.mkv";
    let nfo_filename = "silver.horizon.nfo";
    let payload: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
    let spec = JobSpec {
        name: "Silver Horizon Quick Path".to_string(),
        password: None,
        total_bytes: (payload.len() + 128) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "quick-path-payload@example.com".to_string(),
                }],
            },
            // Nothing in the recovery set describes this, and it is one article
            // short: the file the gate used to stall on.
            FileSpec {
                filename: nfo_filename.to_string(),
                role: FileRole::from_filename(nfo_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "quick-path-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "quick-path-nfo-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(payload_filename.to_string(), payload.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    persist_completed_file_hash(&pipeline, job_id, 0, payload_filename, &payload).await;
    tokio::fs::write(working_dir.join(nfo_filename), vec![7u8; 64])
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    for _ in 0..12 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
    }

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        pipeline.par2_quick_verify_calls >= 1,
        "the quick pass is what should have answered; {}",
        debug_job_state(&pipeline, job_id)
    );
    // Both whole-set readers, since either one of them is the cost this
    // avoids: the placement pass and the repairer's own scan.
    assert_eq!(
        pipeline.par2_authoritative_verify_calls,
        0,
        "a file the recovery set never described must not buy a whole-set read; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_analyze_calls,
        0,
        "nor a whole-set repairer scan; {}",
        debug_job_state(&pipeline, job_id)
    );
}

#[tokio::test]
async fn promoted_recovery_decode_retry_remains_completion_critical() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30948);
    let (_, _, recovery_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Promoted Recovery Decode Retry").await;
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(recovery_segment.file_id.file_index)
        .or_default()
        .promoted = true;

    pipeline.handle_decode_failure(recovery_segment, "bad recovery article", &[], Some(0));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let retry = pipeline
        .retry_rx
        .try_recv()
        .expect("decode failure should schedule a retry");

    assert_eq!(retry.work.segment_id, recovery_segment);
    assert!(retry.work.is_recovery);
    assert!(
        retry.work.completion_critical,
        "decode retry must retain completion-critical recovery provenance"
    );
}

#[tokio::test]
async fn retained_promoted_recovery_buffer_does_not_report_active_fetch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30947);
    let (_, recovery_segment, _) =
        metadata_promotion_job(&mut pipeline, job_id, "Retained Recovery Buffer").await;
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(recovery_segment.file_id.file_index)
        .or_default()
        .promoted = true;

    let bytes = vec![7u8; 64];
    let buffered = BufferedDecodedSegment {
        encoding: SegmentEncoding::Yenc,
        segment_id: recovery_segment,
        decoded_size: bytes.len() as u32,
        data: DecodedChunk::from(bytes.clone()),
        part_crc: par2_rs::checksum::crc32(&bytes),
        part_crc_verified: true,
        yenc_name: "silver-horizon.par2".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        segments: Vec::new(),
    };
    pipeline
        .write_buffers
        .entry(recovery_segment.file_id)
        .or_insert_with(|| WriteReorderBuffer::new(1))
        .insert(64, buffered);

    let job = pipeline
        .list_jobs()
        .into_iter()
        .find(|job| job.job_id == job_id)
        .expect("job remains visible");
    assert!(
        !job.fetching_repair_data,
        "retained decoded recovery bytes are not an active network fetch"
    );
}

/// A metadata candidate that arrived without yielding a set is finished.
///
/// The gate asks for metadata on every entry. A promoted index that completed
/// and parsed into nothing looks exactly like one that was never tried, so
/// without the promoted flag being consulted the same file is enqueued again on
/// every lap, forever.
#[tokio::test]
async fn a_promoted_metadata_candidate_is_never_promoted_twice() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30931);
    let (_, first_segment, _) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Once").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );

    // The index arrives and yields nothing a recovery set can be built from.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(first_segment.file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    assert!(
        pipeline.promote_par2_metadata(job_id),
        "a second candidate remains, so the job is still waiting"
    );
    let queued = drain_promoted_segments(&mut pipeline, job_id);
    assert!(
        !queued.contains(&first_segment),
        "the exhausted candidate must not be enqueued again; queued = {queued:?}"
    );
}

/// The second candidate takes its turn once the first is finished.
#[tokio::test]
async fn an_untried_metadata_candidate_follows_an_exhausted_one() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30932);
    let (_, first_segment, second_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Fallback").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );
    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![second_segment],
        "the untried candidate is the one that goes on the wire"
    );
}

#[tokio::test]
async fn metadata_probe_extracts_a_nonbootstrap_volume_from_its_recovery_queue() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30937);
    let payload_filename = "metadata-queue-payload.bin";
    let selected_volume = "metadata-queue-first.vol00+01.par2";
    let bootstrap_volume = "metadata-queue-second.vol00+01.par2";
    let spec = JobSpec {
        name: "Metadata Queue Extraction".to_string(),
        password: None,
        total_bytes: 256,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-queue-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: selected_volume.to_string(),
                role: FileRole::from_filename(selected_volume),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "metadata-queue-first-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "metadata-queue-first-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: bootstrap_volume.to_string(),
                role: FileRole::from_filename(bootstrap_volume),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-queue-second@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    let selected_file = NzbFileId {
        job_id,
        file_index: 1,
    };

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert_eq!(
            state
                .download_queue
                .count_matching(|work| work.segment_id.file_id == selected_file),
            0,
            "the larger volume is not the no-index bootstrap"
        );
        assert_eq!(
            state
                .recovery_queue
                .count_matching(|work| work.segment_id.file_id == selected_file),
            2
        );
    }

    assert!(pipeline.promote_par2_metadata(job_id));

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        state
            .download_queue
            .count_matching(|work| work.segment_id.file_id == selected_file),
        1,
        "the prefix probe is the only selected-volume copy left queued"
    );
    assert_eq!(
        state
            .recovery_queue
            .count_matching(|work| work.segment_id.file_id == selected_file),
        0,
        "the parked original must not race the promoted probe"
    );

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(selected_file)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    let authentic_par2 = build_test_par2_index("metadata-queue-payload.bin", b"payload", 4);
    pipeline
        .file_prefix_16k
        .insert(selected_file, authentic_par2[..64].to_vec());

    assert!(
        pipeline.promote_par2_metadata(job_id),
        "a contiguous prefix shorter than one complete PAR2 packet advances once"
    );
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![SegmentId {
            file_id: selected_file,
            segment_number: 1,
        }],
        "the second article is the prefix frontier"
    );
}

#[tokio::test]
async fn unavailable_prefix_frontier_does_not_probe_later_carrier_segments() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30938);
    let carrier_filename = "metadata-carrier.vol00+01.par2";
    let carrier_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let missing_segment = SegmentId {
        file_id: carrier_file_id,
        segment_number: 1,
    };
    let spec = JobSpec {
        name: "Committed Metadata Carrier".to_string(),
        password: None,
        total_bytes: 193,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "metadata-carrier-payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 1,
                    message_id: "metadata-carrier-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: carrier_filename.to_string(),
                role: FileRole::from_filename(carrier_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "metadata-carrier-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "metadata-carrier-1@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 2,
                        bytes: 64,
                        message_id: "metadata-carrier-2@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(carrier_file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    pipeline
        .file_prefix_16k
        .insert(carrier_file_id, b"short prefix".to_vec());
    {
        let carrier = pipeline
            .ensure_par2_runtime(job_id)
            .files
            .entry(carrier_file_id.file_index)
            .or_default();
        carrier.discovery = Par2DiscoveryState::PrefixProbeQueued;
        carrier.discovery_probe_ordinals.insert(0);
        carrier.discovery_probe_ordinals.insert(1);
    }
    pipeline.mark_promoted_recovery_segment_unavailable(missing_segment);

    assert!(
        !pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(carrier_file_id)
            .unwrap()
            .is_complete(),
        "precondition: one carrier segment is present and the other is unavailable"
    );
    assert!(
        !pipeline.promote_par2_metadata(job_id),
        "a missing prefix frontier cannot promote a later carrier segment"
    );
    assert!(matches!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .files
            .get(&carrier_file_id.file_index)
            .unwrap()
            .discovery,
        Par2DiscoveryState::Exhausted { .. }
    ));
    assert!(
        pipeline.par2_metadata_discovery_closed(job_id),
        "the completion gate must observe the exhausted sole carrier"
    );
    assert!(
        drain_promoted_segments(&mut pipeline, job_id).is_empty(),
        "the later carrier segment must not be requested"
    );
}

#[tokio::test]
async fn exhausted_optional_prefix_probe_does_not_block_clean_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30949);
    let payload_filename = "optional-prefix-payload.bin";
    let carrier_filename = "optional-prefix.vol00+01.par2";
    let payload: Vec<u8> = (0..64u8).collect();
    let carrier_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let spec = JobSpec {
        name: "Optional Prefix Exhaustion".to_string(),
        password: None,
        total_bytes: 256,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload.len() as u32,
                    message_id: "optional-prefix-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: carrier_filename.to_string(),
                role: FileRole::from_filename(carrier_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "optional-prefix-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "optional-prefix-1@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 2,
                        bytes: 64,
                        message_id: "optional-prefix-2@example.com".to_string(),
                    },
                ],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, payload_filename, &payload).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 0),
        &[],
    );
    pipeline.par2_verified.insert(job_id);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        let carrier = state.assembly.file_mut(carrier_file_id).unwrap();
        carrier.record_placement(1, 64, 64);
        carrier.commit_segment(1, 64).unwrap();
    }
    {
        let carrier = pipeline
            .ensure_par2_runtime(job_id)
            .files
            .entry(carrier_file_id.file_index)
            .or_default();
        carrier.discovery = Par2DiscoveryState::PrefixProbeQueued;
        carrier.discovery_probe_ordinals.insert(0);
    }
    pipeline.mark_promoted_recovery_segment_unavailable(SegmentId {
        file_id: carrier_file_id,
        segment_number: 0,
    });

    assert!(
        !pipeline.promote_par2_metadata(job_id),
        "the broken optional prefix must settle without promoting later articles"
    );
    assert!(matches!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .files
            .get(&carrier_file_id.file_index)
            .unwrap()
            .discovery,
        Par2DiscoveryState::Exhausted { .. }
    ));
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, carrier_file_id.file_index),
        "exhausting a metadata prefix must not promote the optional volume"
    );

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a clean, verified payload must not wait on an optional PAR2 prefix; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);
}

#[tokio::test]
async fn queued_prefix_probe_rearms_only_its_selected_recovery_ordinal_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30939);
    let volume_filename = "metadata-prefix-rearm.vol00+01.par2";
    let volume_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let prior_probe_segment = SegmentId {
        file_id: volume_file_id,
        segment_number: 0,
    };
    let selected_segment = SegmentId {
        file_id: volume_file_id,
        segment_number: 1,
    };
    let unselected_segment = SegmentId {
        file_id: volume_file_id,
        segment_number: 2,
    };
    let spec = JobSpec {
        name: "Prefix Probe Rearm".to_string(),
        password: None,
        total_bytes: 257,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "metadata-prefix-rearm-payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 1,
                    message_id: "metadata-prefix-rearm-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: volume_filename.to_string(),
                role: FileRole::from_filename(volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "metadata-prefix-rearm-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "metadata-prefix-rearm-1@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 2,
                        bytes: 64,
                        message_id: "metadata-prefix-rearm-2@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: "metadata-prefix-bootstrap.vol00+01.par2".to_string(),
                role: FileRole::from_filename("metadata-prefix-bootstrap.vol00+01.par2"),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "metadata-prefix-bootstrap@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        assert_eq!(
            state
                .recovery_queue
                .extract_matching(|work| work.segment_id == prior_probe_segment)
                .len(),
            1,
            "precondition: the earlier probe already left the recovery queue"
        );
        assert_eq!(
            state
                .recovery_queue
                .count_matching(|work| work.segment_id.file_id == volume_file_id),
            2,
            "precondition: only the current probe and later ordinal remain parked"
        );
        let mut blocker = state
            .recovery_queue
            .extract_matching(|work| work.segment_id == unselected_segment);
        assert_eq!(blocker.len(), 1);
        blocker[0].priority = 0;
        state.recovery_queue.push(blocker.pop().unwrap());
        assert_eq!(
            state
                .recovery_queue
                .peek_next_matching(|_| true)
                .map(|work| work.segment_id),
            Some(unselected_segment),
            "precondition: unrelated recovery work must hide the selected probe below the heap head"
        );
    }
    pipeline
        .file_prefix_16k
        .insert(volume_file_id, b"retained earlier prefix".to_vec());
    {
        let probe = pipeline
            .ensure_par2_runtime(job_id)
            .files
            .entry(volume_file_id.file_index)
            .or_default();
        probe.filename = volume_filename.to_string();
        probe.discovery = Par2DiscoveryState::PrefixProbeQueued;
        probe
            .discovery_probe_ordinals
            .insert(prior_probe_segment.segment_number);
        probe
            .discovery_probe_ordinals
            .insert(selected_segment.segment_number);
    }
    assert!(
        !pipeline.promoted_recovery_file_has_pending_work(job_id, volume_file_id.file_index),
        "precondition: only recovery-queue work exists"
    );

    assert!(pipeline.promote_par2_metadata(job_id));
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert_eq!(
            state
                .download_queue
                .count_matching(|work| work.segment_id == selected_segment),
            1,
            "the existing selected probe ordinal is rearmed"
        );
        assert_eq!(
            state.download_queue.count_matching(|work| {
                work.segment_id == selected_segment && work.completion_critical
            }),
            1,
            "the rearmed probe must keep completion priority"
        );
        assert_eq!(
            state
                .download_queue
                .count_matching(|work| work.segment_id == unselected_segment),
            0,
            "no second ordinal is selected"
        );
        assert_eq!(
            state
                .recovery_queue
                .count_matching(|work| work.segment_id == selected_segment),
            0,
            "the selected work moved out of the recovery queue"
        );
        assert_eq!(
            state
                .recovery_queue
                .count_matching(|work| work.segment_id == unselected_segment),
            1,
            "the unselected recovery work remains parked"
        );
    }
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, volume_file_id.file_index),
        "a prefix probe must not promote the full volume"
    );
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .files
            .get(&volume_file_id.file_index)
            .unwrap()
            .discovery_probe_ordinals,
        HashSet::from([
            prior_probe_segment.segment_number,
            selected_segment.segment_number,
        ]),
        "rearming retains the existing bounded probe"
    );

    assert!(pipeline.promote_par2_metadata(job_id));
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        state
            .download_queue
            .count_matching(|work| work.segment_id == selected_segment),
        1,
        "the pending-work guard prevents a duplicate rearm"
    );
    assert_eq!(
        state
            .recovery_queue
            .count_matching(|work| work.segment_id == unselected_segment),
        1,
        "the second promotion still does not select another ordinal"
    );
}

/// Once every candidate has settled, promotion reports that it is finished.
///
/// The callers read `false` as "nothing can produce metadata" and own the
/// terminal failure from there; reporting `true` forever is what kept the job
/// alive with nothing left to try.
#[tokio::test]
async fn exhausted_metadata_candidates_stop_promising_metadata() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30933);
    let (_, first_segment, second_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion Exhausted").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    drain_promoted_segments(&mut pipeline, job_id);
    pipeline.mark_promoted_recovery_segment_unavailable(first_segment);
    assert!(pipeline.promote_par2_metadata(job_id));
    drain_promoted_segments(&mut pipeline, job_id);
    pipeline.mark_promoted_recovery_segment_unavailable(second_segment);

    assert!(
        !pipeline.promote_par2_metadata(job_id),
        "every candidate has settled, so the caller must be allowed to fail the job"
    );
    assert!(
        drain_promoted_segments(&mut pipeline, job_id).is_empty(),
        "and nothing may be enqueued on the way out"
    );
    assert_eq!(
        pipeline.aggregate_par2_failure_message(job_id),
        Some("PAR2 metadata discovery exhausted without finding a recovery set".to_string())
    );
}

/// A metadata probe may reuse an article that ordinary bootstrap already
/// exhausted. The second terminal result must still settle discovery without
/// counting the missing bytes twice.
#[tokio::test]
async fn metadata_probe_observes_a_previously_booked_terminal_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30946);
    let (_, first_segment, second_segment) =
        metadata_promotion_job(&mut pipeline, job_id, "Repeated Metadata Failure").await;

    pipeline.book_failed_segment(first_segment);
    let failed_bytes = pipeline.jobs[&job_id].failed_bytes;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );
    pipeline.book_failed_segment(first_segment);

    assert_eq!(
        pipeline.jobs[&job_id].failed_bytes, failed_bytes,
        "health accounting remains idempotent"
    );
    assert!(
        pipeline.promoted_recovery_file_has_unavailable_segment(job_id, 1),
        "the later metadata role must still observe terminal unavailability"
    );
    assert!(
        pipeline.promote_par2_metadata(job_id),
        "discovery must advance to the next carrier instead of waiting forever"
    );
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![second_segment]
    );
}

#[tokio::test]
async fn a_failed_full_carrier_scan_preserves_prefix_discovery_and_restart_reopens_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(30936);
    let payload_filename = "prefix-discovery.bin";
    let volume_filename = "prefix-discovery.vol00+01.par2";
    let mut par2_bytes = build_test_par2_index(payload_filename, b"prefix-discovery", 8);
    par2_bytes.extend(build_test_par2_index(
        "second-prefix-discovery.bin",
        b"second-prefix-discovery",
        8,
    ));
    let spec = JobSpec {
        name: "Prefix Discovery Failure".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 64,
                    message_id: "prefix-discovery-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: volume_filename.to_string(),
                role: FileRole::from_filename(volume_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "prefix-discovery-volume-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "prefix-discovery-volume-1@example.com".to_string(),
                    },
                ],
            },
        ],
    };

    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    let volume_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(drain_promoted_segments(&mut pipeline, job_id).len(), 1);
    pipeline
        .file_prefix_16k
        .insert(volume_file_id, par2_bytes.clone());

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(drain_promoted_segments(&mut pipeline, job_id).len(), 1);
    let set_ids = pipeline.par2_runtime(job_id).unwrap().ordered_set_ids();
    assert_eq!(set_ids.len(), 2);

    // The carrier completed on the wire, so the full readback is warranted;
    // this fixture deliberately omits the resulting file from disk.
    let file = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .file_mut(volume_file_id)
        .unwrap();
    file.commit_segment(0, 64).unwrap();
    file.commit_segment(1, 64).unwrap();
    assert!(file.is_complete());

    // No carrier exists on disk: the full scan fails after the valid prefix
    // already proved which set this volume belongs to.
    pipeline
        .try_load_par2_metadata(job_id, volume_file_id)
        .await;
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    let file = runtime.files.get(&1).unwrap();
    assert!(matches!(
        &file.discovery,
        Par2DiscoveryState::Exhausted { set_ids: exhausted } if exhausted == &set_ids
    ));
    assert!(
        set_ids
            .iter()
            .all(|set_id| file.metadata_targets_attempted.contains(set_id))
    );
    assert!(pipeline.par2_metadata_discovery_closed(job_id));
    assert!(
        pipeline
            .aggregate_par2_failure_message(job_id)
            .unwrap()
            .contains("metadata discovery exhausted")
    );

    drop(pipeline);
    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
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
    assert!(
        !restored.par2_metadata_discovery_closed(job_id),
        "restart must re-open non-durable discovery instead of trusting stale exhaustion"
    );
}

/// A candidate still on the wire keeps the job waiting without re-enqueuing it.
#[tokio::test]
async fn a_metadata_candidate_still_in_flight_enqueues_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30934);
    let (_, first_segment, _) =
        metadata_promotion_job(&mut pipeline, job_id, "Metadata Promotion In Flight").await;

    assert!(pipeline.promote_par2_metadata(job_id));
    assert_eq!(
        drain_promoted_segments(&mut pipeline, job_id),
        vec![first_segment]
    );

    // Neither complete nor unavailable: the segment is still coming.
    assert!(
        pipeline.promote_par2_metadata(job_id),
        "the job is waiting for something real"
    );
    assert!(
        drain_promoted_segments(&mut pipeline, job_id).is_empty(),
        "but nothing is enqueued a second time"
    );
}

/// A repair's leftovers are shed even when a *later* set settles the job clean.
///
/// Purging by directory difference only ever ran in the repair tail, so a job
/// whose last set needed no repair never reached it: the earlier set's damaged
/// original stayed on disk and shipped with the payload. The aggregate settling
/// is the moment every set that was going to rewrite this directory has done
/// so, whichever kind of verdict happened to close it.
#[tokio::test]
async fn repair_leftovers_are_shed_when_a_clean_set_settles_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30935);
    let payload_filename = "onyx.prairie.mkv";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let (working_dir, _) = incomplete_protected_payload_job(
        &mut pipeline,
        job_id,
        "Leftovers After A Clean Set",
        payload_filename,
        &payload,
    )
    .await;
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );

    // An earlier set repaired here and left the damaged original behind.
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::from([payload_filename.to_string()]));
    let leftover = working_dir.join(format!("{payload_filename}.1"));
    tokio::fs::write(&leftover, &payload).await.unwrap();

    // The job's last set settles clean — no repair tail runs for it at all.
    let par2_set = pipeline.par2_set(job_id).unwrap();
    let set_id = par2_set.recovery_set_id;
    let slice_size = par2_set.slice_size;
    let _ = pipeline
        .settle_par2_set(
            job_id,
            set_id,
            Par2SetSettlementReason::Clean {
                slice_size,
                verification_mode: CleanPar2VerificationMode::Authoritative,
            },
        )
        .await;

    assert!(
        pipeline.par2_verified.contains(&job_id),
        "precondition: the aggregate settled on a clean verdict"
    );
    let delivered: std::collections::HashSet<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !leftover.exists(),
        "the damaged original must not be delivered; directory = {delivered:?}"
    );
    assert!(
        delivered.contains(payload_filename),
        "and the payload itself is untouched; directory = {delivered:?}"
    );
}

/// An index that can still arrive is not a residual to finalize around.
///
/// The shortcut that finalizes a job whose only incomplete files are archive
/// residuals also tolerated an incomplete PAR2 index, on the reasoning that a
/// job which already loaded a set has all the recovery data it is going to use.
/// That reasoning predates a posting carrying more than one set: a second index
/// still on the wire may describe files nothing has verified yet, and taking
/// the shortcut delivers them unchecked.
#[tokio::test]
async fn a_second_index_still_on_the_wire_is_not_an_ignorable_residual() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30936);
    let payload_filename = "silver-horizon.mkv";
    let first_index = "silver-horizon.par2";
    let second_index = "onyx-prairie.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let par2_file = |name: &str, tag: &str| FileSpec {
        filename: name.to_string(),
        role: FileRole::from_filename(name),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 64,
            message_id: format!("residual-{tag}@example.com"),
        }],
    };
    let spec = JobSpec {
        name: "Second Index Still Downloading".to_string(),
        password: None,
        total_bytes: payload.len() as u64 + 128,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "residual-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "residual-payload-1@example.com".to_string(),
                    },
                ],
            },
            par2_file(first_index, "index-a"),
            par2_file(second_index, "index-b"),
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    assert!(
        matches!(
            FileRole::from_filename(second_index),
            FileRole::Par2 { is_index: true, .. }
        ),
        "precondition: the second candidate really is an index"
    );

    // The payload and the first index are in; a set is loaded from the first.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for segment in [0u32, 1] {
            state
                .assembly
                .file_mut(NzbFileId {
                    job_id,
                    file_index: 0,
                })
                .unwrap()
                .commit_segment(segment, 64)
                .unwrap();
        }
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        // The second index is still coming: incomplete, and its article queued.
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 2,
                },
                segment_number: 0,
            },
            message_id: MessageId::new("residual-index-b@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: 64,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, 64, 1),
        &[],
    );
    // The helper models a fully replayed single-set job. This candidate is
    // still on the wire, so leave it unseen as the live promotion path would.
    pipeline
        .ensure_par2_runtime(job_id)
        .files
        .get_mut(&2)
        .unwrap()
        .discovery = Par2DiscoveryState::Unseen;

    assert!(
        !pipeline.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id),
        "a job whose second index is still downloading has not run out of \
         recovery sets to serve, so it must not take the finalization shortcut"
    );
}

// ---------------------------------------------------------------------------
// The whole-file-CRC quick-verification arm.
// ---------------------------------------------------------------------------

/// The arm's whole point: a file the grid never covered, carrying no MD5, is
/// proved against the description's own slice CRC32s without a byte being read.
///
/// The payloads are moved off their paths for the duration of the pass, so a
/// verdict that needed to read them could not have been reached at all.
#[tokio::test]
async fn a_clean_file_settles_from_its_streamed_whole_file_crc_without_reading_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30930);
    let a = misplacement_payload(11);
    let b = misplacement_payload(12);
    let (working_dir, par2_set) = stage_file_crc_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon File CRC Clean",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[a.clone(), b.clone()],
    )
    .await;
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&a),
        true,
        None,
    );
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        1,
        par2_rs::checksum::crc32(&b),
        true,
        None,
    );

    // Neither file has a grid verdict, so the arm above this one cannot answer.
    for file_index in 0..2 {
        assert!(
            pipeline
                .block_crc_verdicts(NzbFileId { job_id, file_index })
                .is_none(),
            "precondition: the grid must not cover these files"
        );
    }

    let hidden_a = working_dir.join("hidden-a");
    let hidden_b = working_dir.join("hidden-b");
    std::fs::rename(working_dir.join("silver-horizon-a.bin"), &hidden_a).unwrap();
    std::fs::rename(working_dir.join("silver-horizon-b.bin"), &hidden_b).unwrap();
    let outcome = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir.clone())
        .await
        .expect("quick verify does not error");
    std::fs::rename(&hidden_a, working_dir.join("silver-horizon-a.bin")).unwrap();
    std::fs::rename(&hidden_b, working_dir.join("silver-horizon-b.bin")).unwrap();

    let (verification, plan, evidence) =
        outcome.expect("the streamed whole-file CRC32s prove both described files");
    assert_eq!(verification.files.len(), 2);
    assert_eq!(plan.exact.len(), 2);
    assert!(plan.unresolved.is_empty());
    assert!(plan.conflicts.is_empty());
    assert_eq!(
        evidence,
        QuickPar2Evidence::FileCrc,
        "the whole-file CRC arm, not the grid and not a digest, decided this set"
    );
}

/// A trusted digest is the stronger instrument. When it contradicts the
/// description the CRC arm binds, nothing here may settle the set.
#[tokio::test]
async fn a_streamed_file_crc_that_contradicts_a_measured_digest_falls_to_the_authoritative_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30931);
    let a = misplacement_payload(13);
    let b = misplacement_payload(14);
    let (working_dir, par2_set) = stage_file_crc_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon File CRC MD5 Conflict",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[a.clone(), b.clone()],
    )
    .await;
    // The CRC binds file 0 to its own description; the digest of the same
    // generation says the bytes are something no description carries.
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&a),
        true,
        Some(par2_rs::checksum::md5(b"a digest no description carries")),
    );
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        1,
        par2_rs::checksum::crc32(&b),
        true,
        None,
    );

    assert!(
        pipeline
            .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
            .await
            .expect("quick verify does not error")
            .is_none(),
        "a measured digest contradicting the CRC binding sends the set to the \
         authoritative pass"
    );
}

/// The length gate. A file whose decoded length is not the described length is
/// never a candidate, wherever its CRC32 might land.
#[tokio::test]
async fn a_streamed_file_crc_at_the_wrong_length_never_binds_a_description() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30932);
    let a = misplacement_payload(15);
    let b = misplacement_payload(16);
    let truncated = a[..a.len() - 8].to_vec();
    let (working_dir, par2_set) = stage_file_crc_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon File CRC Short",
        &[
            ("silver-horizon-a.bin", a.clone()),
            ("silver-horizon-b.bin", b.clone()),
        ],
        &[truncated.clone(), b.clone()],
    )
    .await;
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&truncated),
        true,
        None,
    );
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        1,
        par2_rs::checksum::crc32(&b),
        true,
        None,
    );

    assert!(
        pipeline
            .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
            .await
            .expect("quick verify does not error")
            .is_none(),
        "the short file matches no description's length, so it stays unresolved"
    );
}

/// The in-stream `Damaged` veto runs before every arm, this one included.
#[tokio::test]
async fn a_damaged_in_stream_verdict_vetoes_the_whole_file_crc_arm() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30933);
    let expected = misplacement_payload(17);
    let mut actual = expected.clone();
    actual[3] ^= 0xFF;
    let (working_dir, par2_set) = stage_file_crc_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon File CRC Damaged Veto",
        &[("silver-horizon-a.bin", expected.clone())],
        &[actual.clone()],
    )
    .await;

    // The grid saw the real bytes and contradicted the recovery set.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let wire_crc = par2_rs::checksum::crc32(&actual);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        actual.len() as u64,
        wire_crc,
        true,
        false,
        &[weaver_yenc::Segment {
            file_offset: 0,
            len: actual.len() as u64,
            crc32: wire_crc,
        }],
    );
    assert!(
        pipeline
            .block_crc_verdicts(file_id)
            .is_some_and(|verdicts| {
                verdicts.values().any(|verdict| {
                    matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)
                })
            }),
        "precondition: the grid must call this file Damaged"
    );
    // A CRC that would otherwise bind the description outright.
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&expected),
        true,
        None,
    );

    assert!(
        pipeline
            .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
            .await
            .expect("quick verify does not error")
            .is_none(),
        "a Damaged verdict vetoes the whole-file CRC arm exactly as it vetoes \
         the grid and the digest"
    );
}

/// The streamed CRC32 is a fold of part CRCs. An article that never verified its
/// declared part CRC leaves that fold unattested, and the arm refuses it.
#[tokio::test]
async fn a_file_with_an_unverified_part_crc_does_not_take_the_whole_file_crc_arm() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30934);
    let a = misplacement_payload(18);

    // One fixture, one flag apart, so the refusal is pinned to the flag and not
    // to anything else about the shape.
    let (working_dir, par2_set) = stage_file_crc_shape(
        &mut pipeline,
        job_id,
        "Silver Horizon File CRC Unverified Part",
        &[("silver-horizon-a.bin", a.clone())],
        std::slice::from_ref(&a),
    )
    .await;
    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&a),
        false,
        None,
    );
    assert!(
        pipeline
            .quick_verify_par2_with_placement_for_test(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone()
            )
            .await
            .expect("quick verify does not error")
            .is_none(),
        "an unverified part CRC leaves the streamed fold unattested"
    );

    set_streamed_file_crc(
        &mut pipeline,
        job_id,
        0,
        par2_rs::checksum::crc32(&a),
        true,
        None,
    );
    let (_, _, evidence) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error")
        .expect("the same fixture settles once every part CRC verified");
    assert_eq!(evidence, QuickPar2Evidence::FileCrc);
}

/// The whole arm, driven by the production decode path rather than a hand-set
/// checksum — and the shape that shows why there is nothing left for the MD5
/// substitution to retire.
///
/// The first article lands before the recovery set is served, so it streams an
/// MD5 and closes nothing on the block grid. The second lands after, so the
/// substitution retires the hash — discarding the half-built digest — and the
/// grid closes only the block that article covers. What survives is exactly the
/// arm's input: no digest of any generation, a folded whole-file CRC32, every
/// part CRC verified, and a grid that covers one of the two slices. Before the
/// arm this set had no evidence at all and re-read every byte it had just
/// written.
#[tokio::test]
async fn a_partly_gridded_file_that_streamed_no_md5_settles_from_its_whole_file_crc() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30935);
    let filename = "silver.horizon.mkv";
    let slice_size = 64u64;
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let spec =
        two_segment_standalone_job_spec("Silver Horizon Late Set File CRC", filename, 64, 64);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Article one, before the set is served: nothing to bind to, so the hash is
    // streamed and no block closes.
    submit_decoded_segment(&mut pipeline, file_id, 0, 0, &payload[..64], filename, None).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(filename, &payload, slice_size, 0),
        &[],
    );
    // Article two, after: the substitution retires the hash and the grid closes
    // the one block this article covers.
    submit_decoded_segment(
        &mut pipeline,
        file_id,
        1,
        64,
        &payload[64..],
        filename,
        None,
    )
    .await;

    let checksum = pipeline
        .par2_runtime(job_id)
        .and_then(|runtime| runtime.completed_checksums.get(&file_id))
        .copied()
        .expect("the completed file records a checksum");
    assert!(
        checksum.md5.is_none(),
        "the substitution retired the hash mid-file, so no digest survives"
    );
    assert!(checksum.all_parts_crc_verified);
    assert_eq!(checksum.crc32, par2_rs::checksum::crc32(&payload));

    let par2_set = Arc::clone(pipeline.par2_set(job_id).expect("served recovery set"));
    assert!(
        pipeline
            .in_stream_verified_par2_match(file_id, &par2_set)
            .is_none(),
        "precondition: the grid must not cover every slice, or the arm above \
         this one would answer"
    );

    let (_, plan, evidence) = pipeline
        .quick_verify_par2_with_placement_for_test(job_id, par2_set, working_dir)
        .await
        .expect("quick verify does not error")
        .expect("the streamed whole-file CRC32 proves the described file");
    assert_eq!(plan.exact.len(), 1);
    assert!(plan.unresolved.is_empty());
    assert_eq!(evidence, QuickPar2Evidence::FileCrc);
}

// ---------------------------------------------------------------------------
// Evidence seeding on the authoritative pass.
// ---------------------------------------------------------------------------

/// F2a, on the path a conventional job actually takes. The intact file's
/// in-stream verdicts place its slices, so the analysis reads the damaged file
/// and stops there.
#[tokio::test]
async fn a_damaged_job_reads_only_its_damaged_file_when_the_grid_seeded_evidence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30940);
    // No forcing: this is the shipped default, and the point of the test is
    // that the default is the arm evidence can reach.
    assert!(
        pipeline.stateful_par2_session_forced.is_none(),
        "the default gate is what this test measures"
    );
    let (_, intact, other) = install_seeded_evidence_job(
        &mut pipeline,
        job_id,
        "Silver Horizon Seeded Evidence",
        true,
    )
    .await;

    pipeline.check_job_completion(job_id).await;
    // The read runs on a blocking worker; the byte count is recorded when its
    // verdict lands back on the pipeline task.
    settle_par2_analysis_work(&mut pipeline).await;

    let read = *pipeline
        .par2_authoritative_bytes_read
        .first()
        .expect("the damaged job ran an authoritative analysis");
    assert!(
        read < seeded_evidence_total_payload_bytes(&intact, &other),
        "the seeded intact file must not be read: {read} bytes covers the whole set"
    );
    assert_eq!(
        read,
        other.len() as u64,
        "the analysis reads the damaged file whole and nothing else"
    );
}

/// The same job with the retained session switched off falls to the one-shot
/// repairer, which has no seat for evidence in the crate's published API — so it
/// reads both files. This is the control that pins the saving above to the
/// seeding rather than to anything else about the fixture.
#[tokio::test]
async fn the_one_shot_repairer_reads_every_file_because_it_has_no_seat_for_evidence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(false);
    let job_id = JobId(30941);
    let (_, intact, other) = install_seeded_evidence_job(
        &mut pipeline,
        job_id,
        "Silver Horizon One Shot Control",
        true,
    )
    .await;

    pipeline.check_job_completion(job_id).await;
    // The read runs on a blocking worker; the byte count is recorded when its
    // verdict lands back on the pipeline task.
    settle_par2_analysis_work(&mut pipeline).await;

    let read = *pipeline
        .par2_authoritative_bytes_read
        .first()
        .expect("the damaged job ran an authoritative analysis");
    assert_eq!(
        read,
        seeded_evidence_total_payload_bytes(&intact, &other),
        "the one-shot repairer reads every described file, evidence or not"
    );
}

/// A job the grid never covered is unchanged: every described file is read, and
/// the verdict is reached the way it always was.
#[tokio::test]
async fn a_damaged_job_with_no_in_stream_evidence_still_reads_and_verifies_every_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30942);
    let (_, intact, other) =
        install_seeded_evidence_job(&mut pipeline, job_id, "Silver Horizon No Evidence", false)
            .await;

    pipeline.check_job_completion(job_id).await;
    // The read runs on a blocking worker; the byte count is recorded when its
    // verdict lands back on the pipeline task.
    settle_par2_analysis_work(&mut pipeline).await;

    let read = *pipeline
        .par2_authoritative_bytes_read
        .first()
        .expect("the damaged job ran an authoritative analysis");
    assert_eq!(
        read,
        seeded_evidence_total_payload_bytes(&intact, &other),
        "with nothing seeded the analysis reads the whole set, as before"
    );
}

// ---------------------------------------------------------------------------
// Repairing on the verdict that asked for the recovery.
// ---------------------------------------------------------------------------

/// The defect this section exists for: a job whose recovery arrives must not
/// pay for the damaged-path analysis twice.
///
/// The first pass reads the damaged payload, promotes the volume and parks. The
/// second finds the volume landed and repairs on the verdict the first pass
/// already reached — one authoritative analysis, one retained session, one scan
/// of the sources for the whole ladder.
#[tokio::test]
async fn a_landed_recovery_volume_repairs_on_the_analysis_that_asked_for_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30950);
    let fixture =
        install_parked_recovery_par2_job(&mut pipeline, job_id, "Silver Horizon Parked Recovery")
            .await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading),
        "the analysis promoted the volume and parked on it; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_bytes_read.len(),
        1,
        "one analysis so far"
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);

    land_parked_recovery_volume(&mut pipeline, job_id, &fixture).await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "the repair ran and the job settled; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_bytes_read.len(),
        1,
        "the landed recovery must not buy a second authoritative analysis \
         (bytes read: {:?})",
        pipeline.par2_authoritative_bytes_read
    );
    assert_eq!(
        pipeline.par2_repairs_from_parked_verdict, 1,
        "the repair ran on the parked verdict"
    );
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
    assert_eq!(
        pipeline.par2_session_recovery_merges, 1,
        "the landed volume merged into the retained session instead of \
         rebuilding it"
    );
    assert_eq!(
        pipeline.par2_session_opens, 1,
        "one retained session covered the whole ladder"
    );
    assert_eq!(
        pipeline.par2_session_source_scan_passes, 1,
        "that session read its sources exactly once"
    );
}

/// The shortcut is an optimisation, not a promise about the disk.
///
/// With the retained session forced off — the one-shot shape a session
/// eviction, open failure or restart leaves behind — the parked verdict reaches
/// the repair through par2-rs's scan carry, and the carry is stat-gated. Rewrite
/// the damaged file between the two passes and the gate refuses it: par2-rs
/// rescans on its own and repairs what is actually there, without weaver ever
/// running a second analysis of its own.
#[tokio::test]
async fn a_damaged_file_rewritten_before_the_parked_repair_is_rescanned_by_par2_rs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(false);
    let job_id = JobId(30951);
    let fixture = install_parked_recovery_par2_job(
        &mut pipeline,
        job_id,
        "Silver Horizon Parked Recovery Rewritten",
    )
    .await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading),
        "the analysis promoted the volume and parked on it; {}",
        debug_job_state(&pipeline, job_id)
    );

    // The same slice is still the damaged one, so the verdict's arithmetic
    // still holds — but the bytes behind it are not the bytes the analysis
    // hashed, and nothing weaver holds can tell.
    let slice_bytes = PARKED_RECOVERY_SLICE_SIZE as usize;
    let mut rewritten = fixture.original_payload.clone();
    rewritten[slice_bytes..].fill(0x5A);
    tokio::fs::write(&fixture.payload_path, &rewritten)
        .await
        .unwrap();

    land_parked_recovery_volume(&mut pipeline, job_id, &fixture).await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "par2-rs rescanned and repaired the file as it now is; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_bytes_read.len(),
        1,
        "the rewrite is par2-rs's problem to notice, not a second analysis \
         (bytes read: {:?})",
        pipeline.par2_authoritative_bytes_read
    );
    assert_eq!(
        pipeline.par2_repairs_from_parked_verdict, 1,
        "the repair still ran on the parked verdict"
    );
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
    assert!(
        pipeline.par2_scan_carry_seeded_calls >= 1,
        "the parked repair was seeded with the analysis pass's carry"
    );
}

/// A path-backed session refuses `FileId`-keyed evidence, so the conventional
/// pass names a path — and it has to be the name the file now carries, not the
/// one the NZB gave it.
#[tokio::test]
async fn slice_evidence_is_keyed_to_the_name_a_renamed_file_now_carries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30943);
    let (working_dir, _, _) = install_seeded_evidence_job(
        &mut pipeline,
        job_id,
        "Silver Horizon Renamed Evidence",
        true,
    )
    .await;
    let set_id = pipeline.par2_served_set_id(job_id).expect("a served set");

    let under_source_name = pipeline.in_stream_slice_evidence_paths_for_set(job_id, set_id);
    assert!(
        under_source_name.iter().any(
            |(path, evidence)| path == &working_dir.join(SEEDED_INTACT) && !evidence.is_empty()
        ),
        "the covered file seeds evidence under the name it currently carries"
    );

    // Reconciliation moves the file to the name the description gives it.
    let reconciled = "silver.horizon.e01.reconciled.mkv";
    std::fs::rename(
        working_dir.join(SEEDED_INTACT),
        working_dir.join(reconciled),
    )
    .unwrap();
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: SEEDED_INTACT.to_string(),
                current_filename: reconciled.to_string(),
                canonical_filename: Some(reconciled.to_string()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    let under_current_name = pipeline.in_stream_slice_evidence_paths_for_set(job_id, set_id);
    let seeded_paths: Vec<_> = under_current_name
        .iter()
        .map(|(path, _)| path.clone())
        .collect();
    assert!(
        seeded_paths.contains(&working_dir.join(reconciled)),
        "evidence must follow the file to its effective identity, got {seeded_paths:?}"
    );
    assert!(
        !seeded_paths.contains(&working_dir.join(SEEDED_INTACT)),
        "the vacated source name must not be seeded, got {seeded_paths:?}"
    );
}

#[tokio::test]
async fn a_damaged_job_defers_repairer_analysis_until_its_downloads_drain() {
    // The filesystem damaged-path analysis is a whole-directory authoritative
    // read. It runs on a blocking worker rather than on the pipeline task, so
    // it no longer starves dispatch while it runs — but it is still a full
    // hash of every described file plus a rolling scan of everything else, and
    // while the job has wire work in flight every completing file would submit
    // another one only to rediscover "still waiting". The gate parks the
    // submission until the job's downloads drain; the drain itself is the
    // re-arm.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30188);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Deferred Damaged Analysis".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "deferred-damaged-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "deferred-damaged-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "deferred-damaged-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "deferred-damaged-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Repairing;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    // Wire work still in flight for this job: the damaged path must not run.
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(job_id, 1);

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        pipeline.par2_repairer_analyze_calls, 0,
        "no authoritative analysis while the job's downloads are in flight"
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading),
        "the job parks as downloading — the drain is what re-arms the pass"
    );

    // The drain: the same completion check now runs the single analyze/repair
    // pass and the job settles.
    pipeline.active_downloads = 0;
    pipeline.active_downloads_by_job.remove(&job_id);

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
}

#[tokio::test]
async fn the_one_shot_repairer_chains_scan_carry_between_analysis_and_repair() {
    // With the retained stateful session forced off — the shape a session
    // eviction, open failure, or restart leaves behind — the one-shot
    // repairer runs analysis and repair as two separate constructions. The
    // carry the analysis pass returns must seed the repair pass, so the
    // repair does not re-read what the analysis just hashed.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(false);
    let job_id = JobId(30189);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "One Shot Carry Chain".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "one-shot-carry-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "one-shot-carry-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "one-shot-carry-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "one-shot-carry-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Repairing;
        state.refresh_runtime_lanes_from_status();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state
            .assembly
            .file_mut(file_id)
            .unwrap()
            .commit_segment(1, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 1, true),
        ],
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert_eq!(pipeline.par2_repairer_execute_calls, 1);
    eprintln!(
        "carry counters: seeded={} stashed={} host={}",
        pipeline.par2_scan_carry_seeded_calls,
        pipeline.par2_scan_carry_stashed_calls,
        pipeline.par2_host_carry_builds
    );
    assert!(
        pipeline.par2_scan_carry_seeded_calls >= 1,
        "the repair run must consume the carry an earlier pass produced \
         (seeded={} stashed={} host={})",
        pipeline.par2_scan_carry_seeded_calls,
        pipeline.par2_scan_carry_stashed_calls,
        pipeline.par2_host_carry_builds
    );
    assert!(
        pipeline.par2_scan_carry_stashed_calls >= 1,
        "a completed pass must leave its carry behind for the next one"
    );
}

#[tokio::test]
async fn the_pipeline_task_serves_another_job_while_an_analysis_ticket_runs() {
    // The point of detaching the read: with a damaged job's authoritative
    // analysis outstanding, the pipeline task is free. A second job's
    // completion check runs to its own verdict in the meantime, which is the
    // thing the inline `await` made impossible — it held `&mut self` for the
    // whole read, so every other job's message waited behind it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let damaged_job = JobId(30601);
    insert_damaged_job_ready_for_analysis(&mut pipeline, damaged_job, "analysis-concurrency").await;

    pipeline.check_job_completion(damaged_job).await;
    assert!(
        pipeline.par2_analysis_in_flight.contains_key(&damaged_job),
        "the damaged path must hand its read to a worker rather than run it here"
    );

    // A clean job, submitted and decided with the ticket still outstanding.
    let clean_job = JobId(30602);
    let payload_filename = "Ivory.Meadow.S01E01.mkv";
    let index_filename = "Ivory.Meadow.S01E01.par2";
    let payload: Vec<u8> = (0..128u32).map(|value| (value % 241) as u8).collect();
    let par2_bytes = build_test_par2_index(payload_filename, &payload, 64);
    let spec = split_payload_par2_job_spec(
        "Ivory Meadow Served Meanwhile",
        payload_filename,
        payload.len() as u32,
        index_filename,
        par2_bytes.len() as u32,
    );
    insert_active_job(&mut pipeline, clean_job, spec).await;
    submit_par2_index(&mut pipeline, clean_job, index_filename, &par2_bytes).await;
    submit_split_payload(&mut pipeline, clean_job, payload_filename, &payload).await;
    {
        let state = pipeline.jobs.get_mut(&clean_job).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.check_job_completion(clean_job).await;

    assert!(
        pipeline.par2_verified.contains(&clean_job),
        "the second job must reach its own verdict while the first job's read runs"
    );
    assert!(
        pipeline.par2_analysis_in_flight.contains_key(&damaged_job),
        "the first job's read is still outstanding, so it really did overlap"
    );
}

#[tokio::test]
async fn the_completion_check_does_not_judge_a_job_with_an_analysis_ticket_outstanding() {
    // A completion check that ran while the read was in flight would be
    // judging on the pre-analysis picture — no damage counted, no repair
    // decided — and would settle the job clean. It must instead park, and it
    // must not start a rival read for the same set.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30603);
    insert_damaged_job_ready_for_analysis(&mut pipeline, job_id, "analysis-outstanding").await;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(pipeline.par2_repairer_analyze_calls, 1);
    assert!(pipeline.par2_analysis_in_flight.contains_key(&job_id));

    // Every re-entry while the ticket runs: no verdict, no second read.
    for _ in 0..3 {
        pipeline.check_job_completion(job_id).await;
    }

    assert_eq!(
        pipeline.par2_repairer_analyze_calls, 1,
        "an outstanding ticket must not be joined by a second read for the same set"
    );
    assert!(
        !pipeline.par2_verified.contains(&job_id),
        "the job must not be declared verified before its read lands"
    );
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ),
        "the job must not reach a terminal status while its read is outstanding"
    );

    // The verdict lands and the job settles, so the park really was a park.
    settle_par2_analysis_work(&mut pipeline).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

#[tokio::test]
async fn tearing_a_job_down_forgets_its_outstanding_analysis_ticket() {
    // The retained session the ticket carries belongs to a runtime that is
    // being discarded, and the working directory the verdict names is about to
    // go with it. The done message must find no taker.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30604);
    insert_damaged_job_ready_for_analysis(&mut pipeline, job_id, "analysis-teardown").await;

    pipeline.check_job_completion(job_id).await;
    assert!(pipeline.par2_analysis_in_flight.contains_key(&job_id));

    pipeline.clear_par2_runtime_state(job_id);
    assert!(
        !pipeline.par2_analysis_in_flight.contains_key(&job_id),
        "teardown must forget the ticket"
    );

    let done = next_par2_analysis_done(&mut pipeline).await;
    pipeline.handle_par2_analysis_done(done).await;

    assert!(
        !pipeline.par2_analysis_results.contains_key(&job_id),
        "a forgotten ticket's verdict must not be parked for a later check"
    );
}

#[tokio::test]
async fn a_rebind_between_submit_and_done_discards_the_analysis_verdict() {
    // A verdict names files by path. An identity rewrite between the read
    // starting and its result landing moves those names, so the verdict
    // describes a layout that no longer exists — and acting on it would repair
    // against the wrong file. The rebind forgets the ticket; the completion
    // check submits a fresh read against the new identities.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30605);
    insert_damaged_job_ready_for_analysis(&mut pipeline, job_id, "analysis-rebind").await;

    pipeline.check_job_completion(job_id).await;
    assert!(pipeline.par2_analysis_in_flight.contains_key(&job_id));

    pipeline.invalidate_par2_session_for_identity_rebind(job_id);
    assert!(
        !pipeline.par2_analysis_in_flight.contains_key(&job_id),
        "a rebind must forget the read it invalidated"
    );

    let done = next_par2_analysis_done(&mut pipeline).await;
    pipeline.handle_par2_analysis_done(done).await;

    assert!(
        !pipeline.par2_analysis_results.contains_key(&job_id),
        "the pre-rebind verdict must not be parked"
    );
    assert!(
        !pipeline.par2_verified.contains(&job_id),
        "and it must not have settled the job behind the rebind"
    );

    // The next check starts over rather than standing on the discarded read.
    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.par2_repairer_analyze_calls, 2,
        "a fresh read must be submitted against the rebound identities"
    );
}
