// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// uuencoded files: parts laid at their decoded offsets in order, the park and
// spool that hold parts ahead of their prefix, and losses inside the file.

use super::*;

// Submit one uuencode part.
//
// Every argument is in DECODED units except the job spec's declared sizes,
// which stay encoded exactly as an NZB would carry them — that mismatch is the
// whole point of these tests.
async fn submit_uu_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    damaged: bool,
    ended: bool,
) {
    submit_uu_segment_named(
        pipeline,
        file_id,
        segment_number,
        data,
        crate::pipeline::UuSegmentFacts {
            damaged: damaged,
            ended: ended,
        },
        "silver-horizon.bin",
    )
    .await;
}

// Submit one uuencode part, stating what its `begin` header called the file.
//
// A real post states the name exactly once, on the part that opens the body,
// and continuation parts carry none at all — which is what `""` means here.
async fn submit_uu_segment_named(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    data: &[u8],
    facts: crate::pipeline::UuSegmentFacts,
    begin_name: &str,
) {
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Uu(facts),
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: false,
                part_crc: 0,
                truncation_suspected: false,
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: begin_name.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

// A job whose declared segment sizes are uuencode-encoded (~1.38x the decoded
// bytes), which is what an NZB really carries for a uuencode post.
fn uu_job_spec(decoded_sizes: &[usize]) -> JobSpec {
    let declared: Vec<u32> = decoded_sizes
        .iter()
        .map(|len| (*len as f64 * 1.38).ceil() as u32)
        .collect();
    let mut spec = standalone_job_spec(
        "Silver Horizon UU",
        &[("silver-horizon.bin".to_string(), declared[0])],
    );
    let file = &mut spec.files[0];
    file.segments.clear();
    for (index, bytes) in declared.iter().enumerate() {
        file.segments.push(segment_spec! {
            number: index as u32,
            bytes: *bytes,
            message_id: format!("uu-{index}@example.com"),
        });
    }
    // The post's size is every part it declares.
    spec.total_bytes = declared.iter().map(|bytes| u64::from(*bytes)).sum();
    spec
}

// Number of segments queued for download on a job.
fn queued_segment_count(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.download_queue.len())
        .unwrap_or(0)
}

// A job carrying both encodings: yEnc files whose declared segment sizes run
// ~1.03x their decoded bytes, and one uuencode file whose declared sizes run
// ~1.38x — exactly as the two encodings appear in a real NZB, since the
// `<segment bytes>` attribute is always the ENCODED figure.
fn mixed_encoding_job_spec(
    yenc_files: &[(&str, &[usize])],
    uu_file: (&str, &[usize]),
) -> (JobSpec, Vec<u32>, u32) {
    let mut files = Vec::new();
    let mut yenc_indices = Vec::new();

    for (filename, decoded_sizes) in yenc_files {
        yenc_indices.push(files.len() as u32);
        let index = files.len();
        files.push(FileSpec {
            filename: (*filename).to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: decoded_sizes
                .iter()
                .enumerate()
                .map(|(ordinal, decoded)| {
                    segment_spec! {
                        number: ordinal as u32,
                        bytes: (*decoded as f64 * 1.03).ceil() as u32,
                        message_id: format!("mixed-y{index}-{ordinal}@example.com"),
                    }
                })
                .collect(),
        });
    }

    let uu_index = files.len() as u32;
    files.push(FileSpec {
        filename: uu_file.0.to_string(),
        role: FileRole::Standalone,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: uu_file
            .1
            .iter()
            .enumerate()
            .map(|(ordinal, decoded)| {
                segment_spec! {
                    number: ordinal as u32,
                    bytes: (*decoded as f64 * 1.38).ceil() as u32,
                    message_id: format!("mixed-u-{ordinal}@example.com"),
                }
            })
            .collect(),
    });

    let spec = JobSpec {
        name: "Silver Horizon Mixed".to_string(),
        password: None,
        total_bytes: files
            .iter()
            .flat_map(|file| file.segments.iter())
            .map(|segment| u64::from(segment.bytes))
            .sum(),
        category: None,
        metadata: vec![],
        files,
    };
    (spec, yenc_indices, uu_index)
}

// A job holding one file under `posted_name`, with a recovery set describing
// `described` files by their real names, and `prefix` captured for file 0.
//
// The posted name and the described names are deliberately free to disagree:
// Give the UU spool a known free-space reading so a spill is judged on it.
//
// The pipeline reads spool headroom from the background storage sampler, and
// until that sampler's first probe lands the reading is unknown. A spill with
// no reading is refused and the part requeued, so a test that expects a part
// to spill must not race the sampler: it seeds the reading it wants instead.
fn seed_uu_spool_headroom(pipeline: &mut Pipeline) {
    pipeline.uu_spool_available_bytes_for_test =
        Some(Some(pipeline.uu_spool_min_free_bytes + 64 * 1024 * 1024));
}

// Every part of the file is laid in the job's root, or ruled lost.
fn every_part_settled(pipeline: &Pipeline, file_id: NzbFileId) -> bool {
    let count = pipeline.jobs[&file_id.job_id].spec.files[file_id.file_index as usize]
        .segments
        .len() as u32;
    (0..count).all(|segment_number| {
        pipeline.graph_article_settled(SegmentId {
            file_id,
            segment_number,
        })
    })
}

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
async fn uu_spills_reassemble_in_order_once_the_prefix_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    seed_uu_spool_headroom(&mut pipeline);

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
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: true,
        },
        payload_filename,
    )
    .await;
    submit_uu_segment_named(
        &mut pipeline,
        payload_file_id,
        1,
        &parts[1],
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
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
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
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
}

#[tokio::test]
async fn uu_park_spills_after_resident_budget_and_drains_in_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // The comparison is strict: part 2 stays resident, then part 1 spills.
    pipeline.write_backlog_budget_bytes = 600;
    seed_uu_spool_headroom(&mut pipeline);
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
async fn uu_park_holds_ahead_parts_in_memory_when_free_space_is_unknown() {
    // A filesystem that has never produced a reading is not evidence of low
    // space. Memory parking never touches the disk, so it must proceed; the
    // dispatch cap must stay clear for the same reason.
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90]];
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.uu_spool_available_bytes_for_test = Some(None);
    let job_id = JobId(20183);
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
        Some(1),
        "an unknown reading must not refuse a memory park"
    );
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert_eq!(pipeline.uu_parked_segments, 1);
    assert!(!pipeline.uu_spool_admission_capped(0));
}

#[tokio::test]
async fn uu_park_refuses_to_spill_when_free_space_is_unknown() {
    // Spilling writes to the filesystem, so it needs a reading to judge the
    // reserve against. Without one the part is requeued rather than written
    // blind.
    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90]];
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    pipeline.uu_spool_available_bytes_for_test = Some(None);
    let job_id = JobId(20184);
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
        Some(0)
    );
    assert_eq!(pipeline.uu_spooled_segments, 0);
    assert_eq!(pipeline.uu_parked_segments, 0);
    assert!(!pipeline.uu_spool_root.join(job_id.0.to_string()).exists());
    assert!(
        !pipeline.uu_spool_admission_capped(0),
        "an unknown reading must not refuse memory parking"
    );
    assert!(
        pipeline.uu_spool_dispatch_capped(),
        "a refused spill must stop speculative refetches"
    );

    pipeline.write_backlog_budget_bytes = parts[1].len() + 1;
    assert!(
        !pipeline.uu_spool_dispatch_capped(),
        "freeing memory restores parallel UU dispatch"
    );

    pipeline.write_backlog_budget_bytes = 1;
    assert!(!pipeline.admit_uu_spill(parts[1].len()));
    assert!(pipeline.uu_spool_dispatch_capped());
    pipeline.uu_spool_available_bytes_for_test = Some(Some(
        pipeline.uu_spool_min_free_bytes + parts[1].len() as u64,
    ));
    assert!(
        !pipeline.uu_spool_dispatch_capped(),
        "a recovered capacity reading restores dispatch"
    );
}

#[tokio::test]
async fn uu_spill_admission_debits_the_cached_free_space_reading() {
    // Two spills inside one probe interval must not both see the headroom
    // the single reading reported.
    use crate::operations::{Capacity, CapacityReader, CapacityReading};

    let parts: Vec<Vec<u8>> = vec![vec![b'a'; 80], vec![b'b'; 90], vec![b'c'; 85]];
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    let headroom = pipeline.uu_spool_min_free_bytes + 100;
    let taken = std::time::Instant::now();
    pipeline.uu_spool_capacity = CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes: headroom,
            total_bytes: u64::MAX,
            sampled_at: taken,
            stale: false,
        })
    });
    let job_id = JobId(20185);
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

    submit_uu_segment(&mut pipeline, file_id, 1, &parts[1], false, false).await;
    assert_eq!(
        pipeline.uu_spooled_segments, 1,
        "the first spill fits the headroom"
    );
    assert_eq!(
        pipeline.uu_spool_capacity().best_available_bytes(),
        Some(headroom - parts[1].len() as u64),
        "the admitted spill is debited from the cached reading"
    );

    submit_uu_segment(&mut pipeline, file_id, 2, &parts[2], false, true).await;
    assert_eq!(
        pipeline.uu_spooled_segments, 1,
        "the second spill must see the debited reading and be requeued"
    );
    assert_eq!(
        pipeline.uu_files.get(&file_id).map(|uu| uu.parked.len()),
        Some(1)
    );
}

#[tokio::test]
async fn uu_spilled_replacement_and_displacement_remove_old_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.write_backlog_budget_bytes = 1;
    pipeline.uu_park_max_segments = 2;
    seed_uu_spool_headroom(&mut pipeline);
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
    pipeline.uu_park_max_segments = 1;
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
    seed_uu_spool_headroom(&mut pipeline);
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

    // Every server fails part 3.
    pipeline.book_failed_segment(SegmentId {
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

    // Every part is laid, and nothing is left waiting on a prefix.
    assert!(every_part_settled(&pipeline, file_id));
    let closed = pipeline.uu_files.get(&file_id).expect("uu cursor");
    assert!(closed.parked.is_empty());
}

#[tokio::test]
async fn uu_park_displacement_requeues_instead_of_losing_the_segment() {
    // A displaced part's bytes are dropped, and the download layer already
    // counts that segment as delivered — so without a fresh fetch its data
    // exists nowhere and the cursor wedges forever when it reaches that
    // ordinal. The displacement must put it back on the queue.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.uu_park_max_segments = 1; // force displacement on the second park
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
    pipeline.uu_park_max_segments = 2;
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
async fn uu_park_tail_bounces_behind_a_moving_cursor_without_failing() {
    // A long file fetched by several lanes at once delivers its tail long
    // before the cursor can reach it. With a park smaller than the file, the
    // tail is displaced on every pass and only stops bouncing once the cursor
    // has drained everything below it. Each bounce follows cursor progress, so
    // none of them may count toward the livelock bound: the file must complete
    // without a single charged retry, however many passes it takes.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.uu_park_max_segments = 2;
    let job_id = JobId(20073);
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

    // Every step: the whole tail above the cursor arrives, three times over,
    // then the cursor's own part lands and the park drains what it can. The
    // last ordinal is displaced on every step until the cursor reaches it,
    // far past the per-cursor bound of eight.
    let mut steps = 0;
    loop {
        let cursor = pipeline
            .uu_files
            .get(&file_id)
            .map_or(0, |uu| uu.next_index);
        if cursor as usize >= parts.len() {
            break;
        }
        for _ in 0..3 {
            for index in (cursor + 1..parts.len() as u32).rev() {
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
        submit_uu_segment(
            &mut pipeline,
            file_id,
            cursor,
            &parts[cursor as usize],
            false,
            cursor as usize == parts.len() - 1,
        )
        .await;
        assert!(
            pipeline.decode_retries.is_empty(),
            "a tail displaced behind a moving cursor must never charge retry budget (cursor {cursor})"
        );
        steps += 1;
        assert!(steps <= parts.len(), "the cursor must advance every step");
    }
    assert!(
        steps > 3,
        "the bound must have been crossed for the tail to prove anything"
    );

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
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
        "silver-horizon.bin",
    )
    .await;
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        1,
        &parts[1],
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
        "",
    )
    .await;
    submit_uu_segment_named(
        &mut pipeline,
        file_id,
        2,
        &parts[2],
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: true,
        },
        "",
    )
    .await;

    assert!(every_part_settled(&pipeline, file_id), "the file completed");
    let closed = pipeline.uu_files.get(&file_id).expect("uu cursor");
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
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: true,
        },
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
}

// Submit one yEnc part of a file `file_len` decoded bytes long, as its
// header states.
async fn submit_yenc_part(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    file_len: u64,
) {
    submit_decoded_segment_declaring(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        None,
        false,
        None,
        file_len,
    )
    .await;
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
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
        "silver-horizon.readme.txt",
    )
    .await;
    submit_yenc_part(
        &mut pipeline,
        file_a,
        2,
        (yenc_a[0].len() + yenc_a[1].len()) as u64,
        &yenc_a[2],
        "silver-horizon.s01e01.mkv",
        yenc_a.concat().len() as u64,
    )
    .await;
    submit_uu_segment_named(
        &mut pipeline,
        file_uu,
        2,
        &uu[2],
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: true,
        },
        "",
    )
    .await;
    submit_yenc_part(
        &mut pipeline,
        file_b,
        1,
        yenc_b[0].len() as u64,
        &yenc_b[1],
        "silver-horizon.s01e02.mkv",
        yenc_b.concat().len() as u64,
    )
    .await;
    submit_yenc_part(
        &mut pipeline,
        file_a,
        0,
        0,
        &yenc_a[0],
        "silver-horizon.s01e01.mkv",
        yenc_a.concat().len() as u64,
    )
    .await;
    // Releases the parked uuencode part behind it in the same call.
    submit_uu_segment_named(
        &mut pipeline,
        file_uu,
        1,
        &uu[1],
        crate::pipeline::UuSegmentFacts {
            damaged: false,
            ended: false,
        },
        "",
    )
    .await;
    submit_yenc_part(
        &mut pipeline,
        file_b,
        0,
        0,
        &yenc_b[0],
        "silver-horizon.s01e02.mkv",
        yenc_b.concat().len() as u64,
    )
    .await;
    submit_yenc_part(
        &mut pipeline,
        file_a,
        1,
        yenc_a[0].len() as u64,
        &yenc_a[1],
        "silver-horizon.s01e01.mkv",
        yenc_a.concat().len() as u64,
    )
    .await;

    for (file_id, name, expected) in [
        (file_a, "silver-horizon.s01e01.mkv", yenc_a.concat()),
        (file_b, "silver-horizon.s01e02.mkv", yenc_b.concat()),
        (file_uu, "silver-horizon.readme.txt", uu.concat()),
    ] {
        assert!(
            every_part_settled(&pipeline, file_id),
            "{name} did not complete"
        );
        let written = tokio::fs::read(working_dir.join(name)).await.unwrap();
        assert_eq!(written, expected, "{name} was not written byte-identically");
    }
}
