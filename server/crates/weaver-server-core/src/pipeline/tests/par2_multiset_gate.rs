use super::*;

use crate::pipeline::{
    completion::finalize::check::{
        CleanPar2VerificationMode, Par2SetSettlementReason, SetGateOutcome,
    },
    direct_store::DirectStoreGate,
};

const FIRST_PAYLOAD: &str = "Silver.Horizon.bin";
const SECOND_PAYLOAD: &str = "Ivory.Meadow.bin";
const FIRST_VOLUME: &str = "Silver.Horizon.vol00+01.par2";
const SECOND_VOLUME: &str = "Ivory.Meadow.vol00+01.par2";

fn two_payload_job_spec(name: &str) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: 256,
        category: None,
        metadata: vec![],
        files: [FIRST_PAYLOAD, SECOND_PAYLOAD]
            .into_iter()
            .enumerate()
            .map(|(file_index, filename)| FileSpec {
                filename: filename.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: format!("multiset-gate-{file_index}-0@example.com"),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: format!("multiset-gate-{file_index}-1@example.com"),
                    },
                ],
            })
            .chain([FIRST_VOLUME, SECOND_VOLUME].into_iter().enumerate().map(
                |(volume_index, filename)| FileSpec {
                    filename: filename.to_string(),
                    role: FileRole::from_filename(filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: 1,
                        message_id: format!("multiset-gate-volume-{volume_index}@example.com"),
                    }],
                },
            ))
            .collect(),
    }
}

fn index_only_job_spec(name: &str, indexes: &[(&str, usize)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: indexes.iter().map(|(_, bytes)| *bytes as u64).sum(),
        category: None,
        metadata: vec![],
        files: indexes
            .iter()
            .enumerate()
            .map(|(file_index, (filename, bytes))| FileSpec {
                filename: (*filename).to_string(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: *bytes as u32,
                    message_id: format!("restart-index-{file_index}@example.com"),
                }],
            })
            .collect(),
    }
}

async fn write_damaged_two_segment_payload(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    original: &[u8],
) {
    let mut damaged = original.to_vec();
    damaged[64..].fill(0);
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    tokio::fs::write(working_dir.join(filename), damaged)
        .await
        .unwrap();
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state
        .assembly
        .file_mut(NzbFileId { job_id, file_index })
        .unwrap()
        .commit_segment(0, 64)
        .unwrap();
}

async fn write_present_but_incomplete_assembly(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    original: &[u8],
) {
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    tokio::fs::write(working_dir.join(filename), original)
        .await
        .unwrap();
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state
        .assembly
        .file_mut(NzbFileId { job_id, file_index })
        .unwrap()
        .commit_segment(0, 64)
        .unwrap();
}

fn install_repairable_set(
    pipeline: &mut Pipeline,
    job_id: JobId,
    filename: &str,
    original: &[u8],
    volume_file_index: u32,
) -> par2_rs::RecoverySetId {
    let set = build_repairable_par2_set_for_files(&[(filename, original)], 64, 1);
    let set_id = set.recovery_set_id;
    install_servable_set(
        pipeline,
        job_id,
        set,
        *set_id.as_bytes(),
        &format!("{filename}.par2"),
        volume_file_index,
    );
    let volume_filename = if volume_file_index == 2 {
        FIRST_VOLUME
    } else {
        SECOND_VOLUME
    };
    pipeline.ensure_par2_runtime(job_id).files.insert(
        volume_file_index,
        Par2FileRuntime {
            filename: volume_filename.to_string(),
            recovery_blocks: 1,
            promoted: true,
            recovery_set_id: Some(set_id),
            recovery_set_packets_read: true,
            discovery: Par2DiscoveryState::Parsed {
                set_ids: vec![set_id],
            },
            ..Default::default()
        },
    );
    set_id
}

fn install_unrepairable_set(
    pipeline: &mut Pipeline,
    job_id: JobId,
    filename: &str,
    original: &[u8],
    index_file_index: u32,
) -> par2_rs::RecoverySetId {
    let set = build_repairable_par2_set_for_files(&[(filename, original)], 64, 0);
    let set_id = set.recovery_set_id;
    install_servable_set(
        pipeline,
        job_id,
        set,
        *set_id.as_bytes(),
        &format!("{filename}.par2"),
        index_file_index,
    )
}

fn direct_gate_yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

fn direct_gate_volumes(member_name: &str, payload: &[u8]) -> Vec<(String, Vec<u8>)> {
    let member_crc = par2_rs::checksum::crc32(payload);
    let volume_count = 2;
    let chunk = payload.len().div_ceil(volume_count);
    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;
            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }
            let data_crc = if is_last {
                member_crc
            } else {
                par2_rs::checksum::crc32(part)
            };
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            bytes.extend_from_slice(&build_test_rar_file_header(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(data_crc),
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));
            (format!("Silver.Horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

fn direct_gate_job_spec(volumes: &[(String, Vec<u8>)], index_filename: &str) -> JobSpec {
    let mut files = volumes
        .iter()
        .enumerate()
        .map(|(file_index, (filename, bytes))| FileSpec {
            filename: filename.clone(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: direct_gate_yenc_declared_bytes(bytes.len() as u32),
                message_id: format!("direct-finalize-{file_index}@example.com"),
            }],
        })
        .collect::<Vec<_>>();
    files.push(FileSpec {
        filename: index_filename.to_string(),
        role: FileRole::from_filename(index_filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 1,
            message_id: "direct-finalize-index@example.com".to_string(),
        }],
    });
    JobSpec {
        name: "Silver.Horizon repaired direct finalization".to_string(),
        password: None,
        total_bytes: files
            .iter()
            .flat_map(|file| file.segments.iter())
            .map(|segment| u64::from(segment.bytes))
            .sum(),
        category: None,
        metadata: vec![],
        files,
    }
}

async fn submit_direct_gate_volume(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
) {
    submit_decoded_segment(
        pipeline,
        NzbFileId { job_id, file_index },
        0,
        0,
        bytes,
        filename,
        None,
    )
    .await;
}

async fn settle_set_ids_in_installation_order(reverse: bool) -> (Vec<String>, bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30909);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Onyx.Prairie ordering", &[(FIRST_PAYLOAD.to_string(), 8)]),
    )
    .await;
    let first = (
        placement_par2_file_set(&[(FIRST_PAYLOAD.to_string(), vec![1; 8])]),
        [33; 16],
        "Silver.Horizon.par2",
        1,
    );
    let second = (
        placement_par2_file_set(&[(SECOND_PAYLOAD.to_string(), vec![2; 8])]),
        [34; 16],
        "Ivory.Meadow.par2",
        2,
    );
    let mut install = |set: Par2FileSet, set_id, filename, index_file_index| {
        install_servable_set(
            &mut pipeline,
            job_id,
            set,
            set_id,
            filename,
            index_file_index,
        )
    };
    if reverse {
        install(second.0, second.1, second.2, second.3);
        install(first.0, first.1, first.2, first.3);
    } else {
        install(first.0, first.1, first.2, first.3);
        install(second.0, second.1, second.2, second.3);
    }
    let set_ids = pipeline.par2_servable_set_ids(job_id);
    let sequence = set_ids
        .iter()
        .map(|set_id| {
            pipeline
                .par2_runtime(job_id)
                .unwrap()
                .set_runtime(*set_id)
                .unwrap()
                .summary
                .index_filename
                .clone()
        })
        .collect();
    for set_id in set_ids {
        let _ = pipeline
            .settle_par2_set(job_id, set_id, Par2SetSettlementReason::Repaired)
            .await;
    }
    (sequence, pipeline.par2_verified.contains(&job_id))
}

fn install_servable_set(
    pipeline: &mut Pipeline,
    job_id: JobId,
    mut set: Par2FileSet,
    recovery_set_id: [u8; 16],
    index_filename: &str,
    index_file_index: u32,
) -> par2_rs::RecoverySetId {
    set.recovery_set_id = par2_rs::RecoverySetId::from_bytes(recovery_set_id);
    let set_id = set.recovery_set_id;
    let summary = {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        let set_runtime = runtime.ensure_set_runtime(set_id);
        set_runtime.set = Some(Arc::new(set));
        set_runtime.summary.describes = true;
        set_runtime.summary.index_filename = index_filename.to_string();
        set_runtime.summary.index_file_index = index_file_index;
        set_runtime.summary.described_filenames = set_runtime
            .set
            .as_ref()
            .unwrap()
            .files
            .values()
            .map(|description| description.filename.clone())
            .collect();
        set_runtime.summary.described_bytes = set_runtime
            .set
            .as_ref()
            .unwrap()
            .files
            .values()
            .map(|description| description.length)
            .sum();
        let summary = set_runtime.summary.clone();
        runtime.files.entry(index_file_index).or_default().discovery = Par2DiscoveryState::Parsed {
            set_ids: vec![set_id],
        };
        summary
    };
    assert!(summary.describes);
    set_id
}

#[tokio::test]
async fn aggregate_records_one_verification_result_for_a_two_set_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30901);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Silver.Horizon", &[("Silver.Horizon.bin".to_string(), 8)]),
    )
    .await;

    let first = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[("Silver.Horizon.bin".to_string(), vec![1; 8])]),
        [1; 16],
        "Silver.Horizon.par2",
        1,
    );
    let second = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[("Ivory.Meadow.bin".to_string(), vec![2; 8])]),
        [2; 16],
        "Ivory.Meadow.par2",
        2,
    );

    let verification_count = |pipeline: &Pipeline| {
        pipeline
            .metrics
            .job_lifecycle
            .snapshot()
            .verifications
            .into_iter()
            .map(|(_, count)| count)
            .sum::<u64>()
    };
    let before = verification_count(&pipeline);
    let _ = pipeline
        .settle_par2_set(job_id, first, Par2SetSettlementReason::Repaired)
        .await;
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert_eq!(verification_count(&pipeline), before);
    let _ = pipeline
        .settle_par2_set(job_id, second, Par2SetSettlementReason::Repaired)
        .await;
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_eq!(verification_count(&pipeline), before + 1);
    pipeline.mark_par2_verified(job_id).await;
    assert_eq!(verification_count(&pipeline), before + 1);
}

#[tokio::test]
async fn settlement_gate_requires_a_reason_for_every_settlement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30910);
    let missing = par2_rs::RecoverySetId::from_bytes([90; 16]);

    assert_eq!(
        pipeline
            .settle_par2_set(
                job_id,
                missing,
                Par2SetSettlementReason::Clean {
                    slice_size: 64,
                    verification_mode: CleanPar2VerificationMode::Grid,
                },
            )
            .await,
        SetGateOutcome::Waiting
    );

    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Settlement reasons", &[(FIRST_PAYLOAD.to_string(), 8)]),
    )
    .await;
    let install = |pipeline: &mut Pipeline, seed, filename, file_index| {
        install_servable_set(
            pipeline,
            job_id,
            placement_par2_file_set(&[(FIRST_PAYLOAD.to_string(), vec![seed; 8])]),
            [seed; 16],
            filename,
            file_index,
        )
    };
    let clean = install(&mut pipeline, 91, "clean.par2", 1);
    let repaired = install(&mut pipeline, 92, "repaired.par2", 2);
    let absent = install(&mut pipeline, 93, "absent.par2", 3);

    assert_eq!(
        pipeline
            .settle_par2_set(
                job_id,
                clean,
                Par2SetSettlementReason::Clean {
                    slice_size: 64,
                    verification_mode: CleanPar2VerificationMode::QuickDigest,
                },
            )
            .await,
        SetGateOutcome::Settled
    );
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert_eq!(
        pipeline
            .settle_par2_set(job_id, repaired, Par2SetSettlementReason::Repaired)
            .await,
        SetGateOutcome::Settled
    );
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert_eq!(
        pipeline
            .settle_par2_set(
                job_id,
                absent,
                Par2SetSettlementReason::AbsentUnboundPayload,
            )
            .await,
        SetGateOutcome::Settled
    );
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn index_arrival_order_keeps_the_set_verdict_sequence_and_outcome_stable() {
    let (first_sequence, first_outcome) = settle_set_ids_in_installation_order(false).await;
    let (second_sequence, second_outcome) = settle_set_ids_in_installation_order(true).await;

    assert_eq!(first_sequence, second_sequence);
    assert_eq!(first_sequence, ["Silver.Horizon.par2", "Ivory.Meadow.par2"]);
    assert_eq!(first_outcome, second_outcome);
    assert!(first_outcome);
}

#[tokio::test]
async fn late_servable_set_reopens_the_aggregate_without_clearing_the_first_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30902);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Onyx.Prairie", &[("Onyx.Prairie.bin".to_string(), 8)]),
    )
    .await;

    let first = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[("Onyx.Prairie.bin".to_string(), vec![3; 8])]),
        [3; 16],
        "Onyx.Prairie.par2",
        1,
    );
    let _ = pipeline
        .settle_par2_set(job_id, first, Par2SetSettlementReason::Repaired)
        .await;
    assert!(pipeline.par2_verified.contains(&job_id));

    let second = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[("Ivory.Meadow.bin".to_string(), vec![4; 8])]),
        [4; 16],
        "Ivory.Meadow.par2",
        2,
    );
    pipeline.mark_par2_verified(job_id).await;

    assert!(!pipeline.par2_verified.contains(&job_id));
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(runtime.set_runtime(first).unwrap().settled);
    assert!(!runtime.set_runtime(second).unwrap().settled);

    let reads_before = (
        pipeline.par2_quick_verify_calls,
        pipeline.par2_authoritative_verify_calls,
        pipeline.par2_repairer_analyze_calls,
    );
    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        (
            pipeline.par2_quick_verify_calls,
            pipeline.par2_authoritative_verify_calls,
            pipeline.par2_repairer_analyze_calls,
        ),
        reads_before,
        "reopening for the late set must not read the first settled set again"
    );
}

#[tokio::test]
async fn a_repaired_single_set_finalizes_its_direct_output() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(30910);
    let member_name = "Silver.Horizon.mkv";
    let payload: Vec<u8> = (0..512u32).map(|value| (value % 251) as u8).collect();
    let volumes = direct_gate_volumes(member_name, &payload);
    let index_filename = "Silver.Horizon.par2";
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_gate_job_spec(&volumes, index_filename),
    )
    .await;

    let index_file_index = volumes.len() as u32;
    let set_id = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(
            &volumes
                .iter()
                .map(|(filename, bytes)| (filename.clone(), bytes.clone()))
                .collect::<Vec<_>>(),
        ),
        [91; 16],
        index_filename,
        index_file_index,
    );
    for (file_index, (filename, bytes)) in volumes.iter().enumerate() {
        submit_direct_gate_volume(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: index_file_index,
            })
            .unwrap()
            .commit_segment(0, 1)
            .unwrap();
    }
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_finalized()),
        "the parsed set must hold direct output before its verdict"
    );
    pipeline
        .ensure_par2_runtime(job_id)
        .set_runtime_mut(set_id)
        .unwrap()
        .needed_repair = true;

    let _ = pipeline
        .settle_par2_set(job_id, set_id, Par2SetSettlementReason::Repaired)
        .await;

    assert!(pipeline.par2_verified.contains(&job_id));
    assert_eq!(pipeline.direct_store.finalized_sets, 1);
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| set.is_finalized()),
        "an accepted repair must commit its direct set even though it needed repair"
    );
}

#[tokio::test]
async fn damaged_servable_sets_are_each_repaired_before_the_job_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30903);
    let first_original: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let second_original: Vec<u8> = (0..128u32)
        .map(|value| ((value.wrapping_mul(3) + 17) % 251) as u8)
        .collect();
    insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_job_spec("Silver.Horizon two-set repair"),
    )
    .await;
    write_damaged_two_segment_payload(&mut pipeline, job_id, 0, FIRST_PAYLOAD, &first_original)
        .await;
    write_damaged_two_segment_payload(&mut pipeline, job_id, 1, SECOND_PAYLOAD, &second_original)
        .await;
    install_repairable_set(&mut pipeline, job_id, FIRST_PAYLOAD, &first_original, 2);
    install_repairable_set(&mut pipeline, job_id, SECOND_PAYLOAD, &second_original, 3);
    write_and_complete_file(&mut pipeline, job_id, 2, FIRST_VOLUME, b"A").await;
    write_and_complete_file(&mut pipeline, job_id, 3, SECOND_VOLUME, b"B").await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    let repair_completes = drain_job_events(&mut events, job_id)
        .into_iter()
        .filter(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
        .count();
    assert_eq!(repair_completes, 2, "each damaged set must repair once");
    let output_dir = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Silver.Horizon two-set repair",
    ));
    assert_eq!(
        tokio::fs::read(output_dir.join(FIRST_PAYLOAD))
            .await
            .unwrap(),
        first_original
    );
    assert_eq!(
        tokio::fs::read(output_dir.join(SECOND_PAYLOAD))
            .await
            .unwrap(),
        second_original
    );
}

#[tokio::test]
async fn a_failed_set_waits_for_its_repaired_sibling_before_failing_the_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30904);
    let first_original: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let second_original: Vec<u8> = (0..128u32)
        .map(|value| ((value.wrapping_mul(5) + 29) % 251) as u8)
        .collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_job_spec("Ivory.Meadow deferred failure"),
    )
    .await;
    write_damaged_two_segment_payload(&mut pipeline, job_id, 0, FIRST_PAYLOAD, &first_original)
        .await;
    write_damaged_two_segment_payload(&mut pipeline, job_id, 1, SECOND_PAYLOAD, &second_original)
        .await;
    install_repairable_set(&mut pipeline, job_id, FIRST_PAYLOAD, &first_original, 2);
    install_unrepairable_set(&mut pipeline, job_id, SECOND_PAYLOAD, &second_original, 3);
    write_and_complete_file(&mut pipeline, job_id, 2, FIRST_VOLUME, b"A").await;

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    let error = match job_status_for_assert(&pipeline, job_id) {
        Some(JobStatus::Failed { error, .. }) => error,
        status => panic!("expected the unrecoverable second set to fail, got {status:?}"),
    };
    assert!(error.contains("Ivory.Meadow.bin.par2"));
    assert!(error.contains(SECOND_PAYLOAD));
    assert_eq!(
        drain_job_events(&mut events, job_id)
            .into_iter()
            .filter(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
            .count(),
        1,
        "the repairable first set must finish before its sibling fails the job"
    );
    assert_eq!(
        tokio::fs::read(working_dir.join(FIRST_PAYLOAD))
            .await
            .unwrap(),
        first_original,
        "the completed sibling's repaired bytes remain available after the job fails"
    );
}

#[tokio::test]
async fn a_settled_sets_reconciliation_latch_does_not_spend_its_siblings_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30905);
    let first_original: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let second_original: Vec<u8> = (0..128u32)
        .map(|value| ((value.wrapping_mul(7) + 11) % 251) as u8)
        .collect();
    insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_job_spec("Onyx.Prairie set latch"),
    )
    .await;
    write_present_but_incomplete_assembly(&mut pipeline, job_id, 0, FIRST_PAYLOAD, &first_original)
        .await;
    write_and_complete_file(&mut pipeline, job_id, 1, SECOND_PAYLOAD, &second_original).await;
    let first = install_repairable_set(&mut pipeline, job_id, FIRST_PAYLOAD, &first_original, 2);
    let second = install_repairable_set(&mut pipeline, job_id, SECOND_PAYLOAD, &second_original, 3);
    write_and_complete_file(&mut pipeline, job_id, 2, FIRST_VOLUME, b"A").await;
    write_and_complete_file(&mut pipeline, job_id, 3, SECOND_VOLUME, b"B").await;
    let _ = pipeline
        .settle_par2_set(job_id, first, Par2SetSettlementReason::Repaired)
        .await;
    assert!(
        !pipeline.par2_verified.contains(&job_id),
        "a sibling still owed a pass keeps the aggregate open"
    );
    pipeline.ensure_par2_runtime(job_id).served = Some(first);

    pipeline.check_job_completion(job_id).await;

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert_eq!(
        runtime
            .set_runtime(first)
            .unwrap()
            .post_verdict_reconcile_attempts,
        1
    );
    assert_eq!(
        runtime
            .set_runtime(second)
            .unwrap()
            .post_verdict_reconcile_attempts,
        0
    );
}

#[tokio::test]
async fn an_absent_servable_set_is_skipped_without_a_verification_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30906);
    let original = b"complete".to_vec();
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Ivory.Meadow absent set", &[(FIRST_PAYLOAD.to_string(), 8)]),
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 0, FIRST_PAYLOAD, &original).await;
    install_servable_set(
        &mut pipeline,
        job_id,
        build_repairable_par2_set_for_files(&[("Onyx.Prairie.bin", b"missing")], 64, 0),
        [9; 16],
        "Onyx.Prairie.par2",
        1,
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
}

#[tokio::test]
async fn restart_rebuilds_each_set_and_rederives_each_absent_set_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30907);
    let first_index = build_test_par2_index_for_files(&[(FIRST_PAYLOAD, b"first")], 64);
    let second_index = build_test_par2_index_for_files(&[(SECOND_PAYLOAD, b"second")], 64);
    insert_active_job(
        &mut pipeline,
        job_id,
        index_only_job_spec(
            "Onyx.Prairie restart",
            &[
                ("Silver.Horizon.par2", first_index.len()),
                ("Ivory.Meadow.par2", second_index.len()),
            ],
        ),
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        0,
        "Silver.Horizon.par2",
        &first_index,
    )
    .await;
    write_and_complete_file(&mut pipeline, job_id, 1, "Ivory.Meadow.par2", &second_index).await;

    pipeline.restore_par2_state_from_disk(job_id).await;
    assert_eq!(pipeline.par2_servable_set_ids(job_id).len(), 2);
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .ordered_set_ids()
            .into_iter()
            .all(|set_id| pipeline
                .par2_runtime(job_id)
                .unwrap()
                .set_runtime(set_id)
                .unwrap()
                .set
                .is_some())
    );

    pipeline.check_job_completion(job_id).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

#[tokio::test]
async fn cleanup_after_one_set_settles_keeps_an_unsettled_siblings_described_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30908);
    let first = vec![1; 8];
    let second = vec![2; 8];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        two_payload_job_spec("Silver.Horizon deferred cleanup"),
    )
    .await;
    let first_set = install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(FIRST_PAYLOAD.to_string(), first)]),
        [17; 16],
        "Silver.Horizon.par2",
        2,
    );
    install_servable_set(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(SECOND_PAYLOAD.to_string(), second)]),
        [18; 16],
        "Ivory.Meadow.par2",
        3,
    );
    tokio::fs::write(working_dir.join(SECOND_PAYLOAD), b"keep")
        .await
        .unwrap();
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, HashSet::new());
    let _ = pipeline
        .settle_par2_set(job_id, first_set, Par2SetSettlementReason::Repaired)
        .await;

    assert!(
        !pipeline.par2_verified.contains(&job_id),
        "cleanup waits for the sibling's verdict rather than one set's acceptance"
    );
    assert!(working_dir.join(SECOND_PAYLOAD).exists());
    assert!(
        pipeline.par2_pre_repair_dir_entries.contains_key(&job_id),
        "a per-set acceptance must not run aggregate cleanup while its sibling is unsettled"
    );
}
