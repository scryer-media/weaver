use super::*;

use std::sync::Arc;

use crate::pipeline::direct_store::DirectStoreGate;
use crate::pipeline::direct_store::barrier::BarrierDemand;

const SERVED_SLICE_SIZE: u64 = 64;
const OTHER_SLICE_SIZE: u64 = 96;

fn fixture_bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(29).wrapping_add(seed))
        .collect()
}

fn install_two_parsed_sets(
    pipeline: &mut Pipeline,
    job_id: JobId,
    served: par2_rs::Par2FileSet,
    other: par2_rs::Par2FileSet,
) {
    let served_id = served.recovery_set_id;
    let other_id = other.recovery_set_id;
    assert_ne!(served_id, other_id, "the fixture needs two recovery sets");

    install_test_par2_runtime(pipeline, job_id, served, &[]);
    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.ensure_set_runtime(other_id).set = Some(Arc::new(other));
    assert_eq!(runtime.served, Some(served_id));
}

fn block_cut_segments(file_offset: u64, data: &[u8], block_size: u64) -> Vec<weaver_yenc::Segment> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        let absolute = file_offset + cursor as u64;
        let to_boundary = (block_size - absolute % block_size) as usize;
        let end = (cursor + to_boundary).min(data.len());
        segments.push(weaver_yenc::Segment {
            file_offset: absolute,
            len: (end - cursor) as u64,
            crc32: par2_rs::checksum::crc32(&data[cursor..end]),
        });
        cursor = end;
    }
    segments
}

#[tokio::test]
async fn a_non_served_file_records_on_its_own_slice_grid() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(32201);
    let served_bytes = fixture_bytes(3, SERVED_SLICE_SIZE as usize);
    let other_bytes = fixture_bytes(7, (OTHER_SLICE_SIZE * 2) as usize);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Birch Lantern Grid",
            &[("birch.lantern.bin".to_string(), other_bytes.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[("copper.pond.bin", served_bytes.as_slice())],
        SERVED_SLICE_SIZE,
        0,
    );
    let other = build_repairable_par2_set_for_files(
        &[("birch.lantern.bin", other_bytes.as_slice())],
        OTHER_SLICE_SIZE,
        0,
    );
    let other_id = other.recovery_set_id;
    install_two_parsed_sets(&mut pipeline, job_id, served, other);

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_and_complete_file(
        &mut pipeline,
        job_id,
        file_id.file_index,
        "birch.lantern.bin",
        &other_bytes,
    )
    .await;
    let crc32 = par2_rs::checksum::crc32(&other_bytes);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        other_bytes.len() as u64,
        crc32,
        true,
        false,
        &block_cut_segments(0, &other_bytes, OTHER_SLICE_SIZE),
    );

    assert_eq!(
        pipeline.par2_block_size_for_file(file_id),
        std::num::NonZeroU64::new(OTHER_SLICE_SIZE),
    );
    let binding = pipeline
        .resolve_par2_file_binding(file_id)
        .expect("the non-served description binds this file");
    assert_eq!(binding.recovery_set_id, other_id);
    let verdicts = pipeline
        .block_crc_verdicts(file_id)
        .expect("the non-served grid closes both blocks");
    assert_eq!(verdicts.len(), 2);
    assert!(verdicts.values().all(|verdict| matches!(
        verdict,
        crate::pipeline::integrity::BlockVerdict::Intact {
            independently_covered: true
        }
    )));
}

#[tokio::test]
async fn a_served_file_keeps_its_grid_when_another_set_is_present() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(32202);
    let served_bytes = fixture_bytes(11, (SERVED_SLICE_SIZE * 2) as usize);
    let other_bytes = fixture_bytes(13, OTHER_SLICE_SIZE as usize);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Cinder Orchard Grid",
            &[("cinder.orchard.bin".to_string(), served_bytes.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[("cinder.orchard.bin", served_bytes.as_slice())],
        SERVED_SLICE_SIZE,
        0,
    );
    let served_id = served.recovery_set_id;
    let other = build_repairable_par2_set_for_files(
        &[("dawn.rill.bin", other_bytes.as_slice())],
        OTHER_SLICE_SIZE,
        0,
    );
    install_two_parsed_sets(&mut pipeline, job_id, served, other);

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_and_complete_file(
        &mut pipeline,
        job_id,
        file_id.file_index,
        "cinder.orchard.bin",
        &served_bytes,
    )
    .await;
    let crc32 = par2_rs::checksum::crc32(&served_bytes);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        served_bytes.len() as u64,
        crc32,
        true,
        false,
        &block_cut_segments(0, &served_bytes, SERVED_SLICE_SIZE),
    );

    assert_eq!(
        pipeline
            .resolve_par2_file_binding(file_id)
            .expect("the served file binds")
            .recovery_set_id,
        served_id
    );
    assert_eq!(
        pipeline
            .block_crc_verdicts(file_id)
            .expect("the served grid still closes")
            .len(),
        2
    );
}

#[tokio::test]
async fn one_file_can_close_two_independent_slice_grids() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(32204);
    let filename = "ember.delta.bin";
    let bytes = fixture_bytes(19, 509);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Ember Delta Shared Grids",
            &[(filename.to_string(), bytes.len() as u32)],
        ),
    )
    .await;
    let first =
        build_repairable_par2_set_for_files(&[(filename, bytes.as_slice())], SERVED_SLICE_SIZE, 0);
    let second =
        build_repairable_par2_set_for_files(&[(filename, bytes.as_slice())], OTHER_SLICE_SIZE, 0);
    install_two_parsed_sets(&mut pipeline, job_id, first.clone(), second.clone());
    pipeline.refresh_par2_checkpoint_plan(job_id);

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_and_complete_file(&mut pipeline, job_id, 0, filename, &bytes).await;
    let plan = pipeline.par2_checkpoint_plan(job_id);
    assert!(matches!(plan, weaver_yenc::CheckpointPlan::Multi(_)));
    let mut crc = weaver_yenc::SegmentedCrc32::new(0, plan.clone());
    crc.update(&bytes);
    let (part_crc, segments) = crc.finish_article();
    pipeline.note_block_crc_segments_for_plan(
        file_id,
        &plan,
        0,
        bytes.len() as u64,
        part_crc,
        true,
        false,
        &segments,
    );
    pipeline
        .block_crcs
        .note_file_len(file_id, bytes.len() as u64);

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "job-wide binding remains ambiguous across recovery sets"
    );
    assert!(
        pipeline
            .in_stream_verified_par2_match(file_id, &first)
            .is_some()
    );
    assert!(
        pipeline
            .in_stream_verified_par2_match(file_id, &second)
            .is_some()
    );
}

#[tokio::test]
async fn a_rebound_file_drops_blocks_cut_on_the_old_grid() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(32203);
    let bytes = fixture_bytes(17, (OTHER_SLICE_SIZE * 2) as usize);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Dune Quill Rebind",
            &[("dune.quill.bin".to_string(), bytes.len() as u32)],
        ),
    )
    .await;
    let old = build_repairable_par2_set_for_files(
        &[("dune.quill.bin", bytes.as_slice())],
        SERVED_SLICE_SIZE,
        0,
    );
    let old_id = old.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, old, &[]);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_and_complete_file(
        &mut pipeline,
        job_id,
        file_id.file_index,
        "dune.quill.bin",
        &bytes,
    )
    .await;
    let crc32 = par2_rs::checksum::crc32(&bytes);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        bytes.len() as u64,
        crc32,
        true,
        false,
        &block_cut_segments(0, &bytes, SERVED_SLICE_SIZE),
    );
    assert!(pipeline.block_crc_verdicts(file_id).is_some());

    let rebound = build_repairable_par2_set_for_files(
        &[("dune.quill.bin", bytes.as_slice())],
        OTHER_SLICE_SIZE,
        0,
    );
    let rebound_id = rebound.recovery_set_id;
    {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.set_runtime_mut(old_id).expect("the old set").set = None;
        runtime.ensure_set_runtime(rebound_id).set = Some(Arc::new(rebound));
        runtime.served = Some(rebound_id);
    }

    assert!(
        pipeline.block_crc_verdicts(file_id).is_none(),
        "an old-grid claim must not be read against the rebound set"
    );
    pipeline.note_block_crc_segments(
        file_id,
        0,
        bytes.len() as u64,
        crc32,
        true,
        false,
        &block_cut_segments(0, &bytes, OTHER_SLICE_SIZE),
    );
    assert_eq!(
        pipeline
            .block_crc_verdicts(file_id)
            .expect("the new grid replaces the old evidence")
            .len(),
        2
    );
}

fn yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

fn one_volume_store_set(
    volume_name: &str,
    member_name: &str,
    payload: &[u8],
) -> Vec<(String, Vec<u8>)> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEST_RAR5_SIG);
    bytes.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    bytes.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0,
        payload.len() as u64,
        payload.len() as u64,
        Some(par2_rs::checksum::crc32(payload)),
    ));
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&build_test_rar_end_header(false));
    vec![(volume_name.to_string(), bytes)]
}

fn direct_store_job_spec(name: &str, volumes: &[(String, Vec<u8>)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: volumes
            .iter()
            .map(|(_, bytes)| u64::from(yenc_declared_bytes(bytes.len() as u32)))
            .sum(),
        category: None,
        metadata: vec![],
        files: volumes
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: yenc_declared_bytes(bytes.len() as u32),
                    message_id: format!("multiset-{index}@example.com"),
                }],
            })
            .collect(),
    }
}

fn direct_store_job_spec_with_par2_indexes(
    volumes: &[(String, Vec<u8>)],
    served_index: &[u8],
    other_index: &[u8],
) -> JobSpec {
    let mut spec = direct_store_job_spec("Ember Frost Archives", volumes);
    for (filename, bytes, message_id) in [
        (
            "ember.cove.par2",
            served_index,
            "multiset-served-index@example.com",
        ),
        (
            "frost.grove.par2",
            other_index,
            "multiset-other-index@example.com",
        ),
    ] {
        spec.total_bytes += u64::from(yenc_declared_bytes(bytes.len() as u32));
        spec.files.push(FileSpec {
            filename: filename.to_string(),
            role: FileRole::from_filename(filename),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: yenc_declared_bytes(bytes.len() as u32),
                message_id: message_id.to_string(),
            }],
        });
    }
    spec
}

async fn submit_direct_volume(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    filename: &str,
    bytes: &[u8],
    slice_size: u64,
) {
    submit_decoded_segment_with_segments(
        pipeline,
        NzbFileId { job_id, file_index },
        0,
        0,
        bytes,
        filename,
        None,
        true,
        Some(block_cut_segments(0, bytes, slice_size)),
    )
    .await;
}

async fn two_direct_sets_with_indexes(
    temp_dir: &TempDir,
    job_id: JobId,
) -> (
    Pipeline,
    PathBuf,
    par2_rs::Par2FileSet,
    par2_rs::Par2FileSet,
    Vec<(String, Vec<u8>)>,
) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.stateful_par2_session_forced = Some(true);

    let served_volume = one_volume_store_set(
        "ember.cove.part01.rar",
        "ember.cove.member.bin",
        &fixture_bytes(23, 384),
    );
    let other_volume = one_volume_store_set(
        "frost.grove.part01.rar",
        "frost.grove.member.bin",
        &fixture_bytes(31, 384),
    );
    let holding_volume = one_volume_store_set(
        "garnet.field.part01.rar",
        "garnet.field.member.bin",
        &fixture_bytes(37, 384),
    );
    let mut volumes = served_volume;
    volumes.extend(other_volume);
    volumes.extend(holding_volume);

    let served_index = build_test_par2_index_for_files(
        &[(volumes[0].0.as_str(), volumes[0].1.as_slice())],
        SERVED_SLICE_SIZE,
    );
    let other_index = build_test_par2_index_for_files(
        &[(volumes[1].0.as_str(), volumes[1].1.as_slice())],
        OTHER_SLICE_SIZE,
    );
    let served = par2_rs::Par2FileSet::from_files(&[served_index.as_slice()])
        .expect("the served index parses");
    let other = par2_rs::Par2FileSet::from_files(&[other_index.as_slice()])
        .expect("the other index parses");
    let spec = direct_store_job_spec_with_par2_indexes(&volumes, &served_index, &other_index);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let served_path = working_dir.join("ember.cove.par2");
    let other_path = working_dir.join("frost.grove.par2");
    std::fs::write(&served_path, &served_index).unwrap();
    std::fs::write(&other_path, &other_index).unwrap();
    install_two_parsed_sets(&mut pipeline, job_id, served.clone(), other.clone());

    submit_direct_volume(
        &mut pipeline,
        job_id,
        0,
        &volumes[0].0,
        &volumes[0].1,
        SERVED_SLICE_SIZE,
    )
    .await;
    submit_direct_volume(
        &mut pipeline,
        job_id,
        1,
        &volumes[1].0,
        &volumes[1].1,
        OTHER_SLICE_SIZE,
    )
    .await;
    let other_file_id = NzbFileId {
        job_id,
        file_index: 1,
    };
    let other_crc32 = par2_rs::checksum::crc32(&volumes[1].1);
    pipeline.note_block_crc_segments(
        other_file_id,
        0,
        volumes[1].1.len() as u64,
        other_crc32,
        true,
        false,
        &block_cut_segments(0, &volumes[1].1, OTHER_SLICE_SIZE),
    );
    pipeline
        .block_crcs
        .note_file_len(other_file_id, volumes[1].1.len() as u64);
    (pipeline, working_dir, served, other, volumes)
}

#[tokio::test]
async fn a_non_served_direct_set_uses_its_own_grid_evidence_in_the_session() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(32204);
    let (mut pipeline, working_dir, _served, other, _) =
        two_direct_sets_with_indexes(&temp_dir, job_id).await;
    let other_id = other.recovery_set_id;

    let evidence = pipeline.in_stream_slice_evidence_for_set(job_id, other_id);
    assert!(
        !evidence.is_empty(),
        "the non-served direct volume must yield its own slice evidence",
    );
    let before = pipeline.direct_session_pass_calls;
    let verification = pipeline
        .verify_direct_sets_quietly(job_id, Arc::new(other), working_dir)
        .await
        .expect("the non-served direct overlay has a verdict");
    assert!(
        !verification.needs_repair(),
        "the direct bytes and their own PAR2 description agree"
    );
    assert_eq!(
        pipeline.direct_session_pass_calls,
        before + 1,
        "the non-served set must take the no-read retained-session path"
    );
}

#[tokio::test]
async fn direct_sets_stay_isolated_by_their_recovery_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(32205);
    let (mut pipeline, working_dir, served, other, _) =
        two_direct_sets_with_indexes(&temp_dir, job_id).await;

    let served_verification = pipeline
        .verify_direct_sets_quietly(job_id, Arc::new(served.clone()), working_dir.clone())
        .await
        .expect("the served direct set verifies through its own overlay");
    assert!(!served_verification.needs_repair());
    let sessions_before_other = pipeline.direct_session_pass_calls;
    let other_verification = pipeline
        .verify_direct_sets_quietly(job_id, Arc::new(other.clone()), working_dir.clone())
        .await
        .expect("the non-served direct set verifies through its own overlay");
    assert!(!other_verification.needs_repair());
    assert_eq!(
        pipeline.direct_session_pass_calls,
        sessions_before_other + 1,
        "the non-served set takes its own no-read session, not the served set's"
    );

    let overlay = pipeline
        .direct_par2_overlay_for_set(job_id, served.recovery_set_id)
        .expect("the served overlay");
    let damaged = par2_rs::VerificationResult {
        files: overlay
            .volumes
            .iter()
            .map(|volume| par2_rs::verify::FileVerification {
                file_id: volume.par2_file_id,
                filename: "ember.cove.part01.rar".to_string(),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false],
                missing_slice_count: 1,
            })
            .collect(),
        recovery_blocks_available: 0,
        total_missing_blocks: overlay.volumes.len() as u32,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    assert!(
        pipeline
            .demote_direct_sets_with_par2_damage_for_set(job_id, served.recovery_set_id, &damaged)
            .await,
        "damage in one set demotes that set's direct archive set"
    );
    assert!(
        !pipeline.is_direct_source_file(NzbFileId {
            job_id,
            file_index: 0
        }),
        "the damaged set demotes"
    );
    assert!(
        pipeline.is_direct_source_file(NzbFileId {
            job_id,
            file_index: 1
        }),
        "damage in another recovery set must not demote or materialize this set"
    );
}

#[test]
fn retained_sessions_use_job_and_recovery_set_as_the_lru_key() {
    let job_id = JobId(32206);
    let older = (job_id, par2_rs::RecoverySetId::from_bytes([1; 16]));
    let protected = (job_id, par2_rs::RecoverySetId::from_bytes([2; 16]));
    let now = std::time::Instant::now();

    assert_eq!(
        crate::pipeline::repair::par2::select_par2_session_eviction(
            [
                (older, true, Some(now - std::time::Duration::from_secs(2))),
                (
                    protected,
                    true,
                    Some(now - std::time::Duration::from_secs(1))
                ),
            ],
            protected,
        ),
        Some(older)
    );
}

#[tokio::test]
async fn restart_rebuilds_each_direct_overlay_for_its_own_recovery_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(32207);
    let (mut pipeline, working_dir, served, other, volumes) =
        two_direct_sets_with_indexes(&temp_dir, job_id).await;
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;

    let served_index = build_test_par2_index_for_files(
        &[(volumes[0].0.as_str(), volumes[0].1.as_slice())],
        SERVED_SLICE_SIZE,
    );
    let other_index = build_test_par2_index_for_files(
        &[(volumes[1].0.as_str(), volumes[1].1.as_slice())],
        OTHER_SLICE_SIZE,
    );
    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored.direct_store.set_gate(DirectStoreGate::Enabled);
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: direct_store_job_spec_with_par2_indexes(&volumes, &served_index, &other_index),
            complete_files: std::collections::HashSet::new(),
            file_progress: std::collections::HashMap::new(),
            detected_archives: std::collections::HashMap::new(),
            file_identities: std::collections::HashMap::new(),
            extracted_members: std::collections::HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .expect("the direct sets restore");
    install_two_parsed_sets(&mut restored, job_id, served.clone(), other.clone());

    let served_overlay = restored
        .direct_par2_overlay_for_set(job_id, served.recovery_set_id)
        .expect("the served overlay rebuilds after restart");
    let other_overlay = restored
        .direct_par2_overlay_for_set(job_id, other.recovery_set_id)
        .expect("the other overlay rebuilds after restart");
    assert_eq!(served_overlay.recovery_set_id, served.recovery_set_id);
    assert_eq!(other_overlay.recovery_set_id, other.recovery_set_id);
    assert_eq!(served_overlay.volumes.len(), 1);
    assert_eq!(other_overlay.volumes.len(), 1);
    assert_ne!(
        served_overlay.volumes[0].par2_file_id,
        other_overlay.volumes[0].par2_file_id
    );
}
