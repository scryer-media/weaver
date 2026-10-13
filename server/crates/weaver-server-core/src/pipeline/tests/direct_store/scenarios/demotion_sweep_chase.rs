// A demoted set whose reconstruction sweep is still running, and the chase
// that set's handback could arm while it is.

use super::*;
use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;

// Two articles of one volume, the first carrying a damaged payload byte. The
// article's own CRC agrees with what was posted, so only the member's CRC —
// checked when the second article closes the chain — refuses the set.
fn damaged_single_volume_set(member_name: &str) -> Vec<(String, Vec<u8>)> {
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 181) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 1);
    let (_, first_end) = article_extent(volumes[0].1.len(), 0, 2);
    volumes[0].1[first_end - 9] ^= 0xFF;
    volumes
}

// [`submit_volume_article`] without settling the demotion it may cause, so
// the test can hand the sweep's messages to the actor one at a time.
async fn decode_article_leaving_the_sweep_unsettled(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    segment_number: u32,
) {
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let (filename, bytes) = &volumes[0];
    let (start, end) = article_extent(bytes.len(), segment_number, 2);
    let data = &bytes[start..end];
    let total_segments = pipeline.jobs[&job_id]
        .assembly
        .file(file_id)
        .expect("active test file assembly")
        .total_segments();
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: bytes.len() as u64,
                    part: Some(segment_number + 1),
                    total: Some(total_segments),
                    begin: Some(start as u64 + 1),
                    end: Some(end as u64),
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(data),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: filename.clone(),
                checkpoint_plan: pipeline.par2_checkpoint_plan(job_id),
                segments: vec![weaver_yenc::Segment {
                    file_offset: start as u64,
                    len: data.len() as u64,
                    crc32: par2_rs::checksum::crc32(data),
                }],
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
    settle_direct_verification_read(pipeline, job_id).await;
}

// Receives the sweep's next message and hands it to the actor, returning
// which volume it reported, or `None` for the finish.
async fn hand_back_next_swept_message(pipeline: &mut Pipeline) -> Option<u32> {
    let done = pipeline
        .direct_demotion_done_rx
        .recv()
        .await
        .expect("the demotion completion channel should stay open");
    let reported = match &done.progress {
        crate::pipeline::DirectDemotionProgress::Volume(outcome) => Some(outcome.volume_index),
        crate::pipeline::DirectDemotionProgress::Finished { .. } => None,
    };
    pipeline.handle_direct_demotion_done(done).await;
    reported
}

#[tokio::test]
async fn a_demoted_set_is_not_chased_while_its_sweep_is_outstanding() {
    let member_name = "Copper.Lantern.S03E02.mkv";
    let volumes = damaged_single_volume_set(member_name);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41120);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let spec = direct_store_job_spec("Copper Lantern", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    for segment_number in 0..2 {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id,
                segment_number,
            },
        );
        decode_article_leaving_the_sweep_unsettled(&mut pipeline, job_id, &volumes, segment_number)
            .await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)"),
        "the whole-member gate should have demoted the set, got {shape}"
    );
    let set_name = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the demoted set stays registered")
        .set_name()
        .to_string();
    assert!(pipeline.demotion_sweep_owns_file(file_id));

    // The sweep reports the rebuilt volume before it reports its finish, and
    // the actor sees only the first of the two here. The handback writes the
    // article the demotion handed off, which completes the volume.
    assert_eq!(hand_back_next_swept_message(&mut pipeline).await, Some(0));
    assert!(
        pipeline.jobs[&job_id]
            .assembly
            .file(file_id)
            .is_some_and(|file| file.is_complete()),
        "the handback should have completed the volume, which is what tries to arm its set"
    );
    assert!(
        pipeline
            .direct_demotion_in_flight
            .get(&job_id)
            .is_some_and(|sets| sets.contains_key(&0)),
        "the sweep has not finished, so its ticket is still open"
    );
    assert!(
        !pipeline.direct_unpack.is_armed(job_id, &set_name),
        "no chase may be armed over a set whose sweep is still outstanding"
    );

    // The ticket lands, and the completion replay may chase the set now.
    assert_eq!(hand_back_next_swept_message(&mut pipeline).await, None);
    assert!(pipeline.direct_demotion_in_flight.is_empty());
    assert!(
        pipeline.direct_unpack.is_armed(job_id, &set_name),
        "the refusal while the sweep ran must not outlive it"
    );
    pipeline.direct_unpack_shutdown("test teardown").await;
}

#[tokio::test]
async fn demotion_rejects_par2_analysis_receipts_on_both_sides_of_the_handback() {
    for receipt_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let job = JobId(41121);
        let volumes = single_member_store_set("fixture.mkv", &[0x31; 3000], 1);
        let spec = direct_store_job_spec("Demotion analysis fence", &volumes);
        insert_active_job(&mut pipeline, job, spec).await;
        submit_volume_article(&mut pipeline, job, &volumes, 0, 0).await;
        assert!(pipeline.direct_store.set(job, 0).is_some());

        let set_id = par2_rs::RecoverySetId::from_bytes([0x21; 16]);
        pipeline.par2_analysis_in_flight.insert(
            job,
            crate::pipeline::Par2AnalysisWork {
                work_id: 7,
                recovery_set_id: set_id,
                submitted_at: std::time::Instant::now(),
            },
        );
        pipeline
            .metrics
            .verify_active
            .fetch_add(1, Ordering::Relaxed);
        let done = crate::pipeline::Par2AnalysisWorkDone {
            job_id: job,
            work_id: 7,
            recovery_set_id: set_id,
            outcome:
                crate::pipeline::completion::finalize::check::Par2AnalysisTicketOutcome::TaskFailed(
                    "old source image".to_string(),
                ),
        };
        let mut done = Some(done);
        if receipt_first {
            pipeline
                .handle_par2_analysis_done(done.take().unwrap())
                .await;
            assert!(pipeline.par2_analysis_results.contains_key(&job));
        }
        pipeline
            .demote_direct_set(job, 0, DemotionReason::HoldsBudgetExceeded)
            .await;
        assert!(pipeline.direct_demotion_in_flight.contains_key(&job));
        assert!(
            !pipeline.par2_analysis_in_flight.contains_key(&job),
            "receipt_first={receipt_first}"
        );
        assert!(
            !pipeline.par2_analysis_results.contains_key(&job),
            "receipt_first={receipt_first}"
        );
        if let Some(done) = done {
            pipeline.handle_par2_analysis_done(done).await;
        }
        assert!(!pipeline.par2_analysis_results.contains_key(&job));
        settle_direct_demotion_work(&mut pipeline).await;
    }
}
