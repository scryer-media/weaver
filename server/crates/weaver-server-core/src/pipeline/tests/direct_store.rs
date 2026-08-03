//! Direct-store routing, phase 4 (plan 135).
//!
//! The spine is differential: the identical job gate is run with routing on and
//! off, and the outputs must be byte-identical. With routing on, no source
//! volume may ever appear on disk.

use super::*;

use crate::pipeline::direct_store::DirectStoreGate;

/// A real NZB's `<segment bytes=…>` is the yEnc-**encoded** article size, about
/// 3% larger than the decoded payload. Every fixture here declares inflated
/// sizes so no routing arithmetic can accidentally be right by reading an NZB
/// total: all of it goes through `file_offset`/`decoded_size`.
fn yenc_declared_bytes(decoded_len: u32) -> u32 {
    decoded_len + decoded_len.div_ceil(32) + 2
}

/// One stored member split across `volume_count` volumes, RAR5, unencrypted.
///
/// Non-final parts carry the packed CRC32 of *their* bytes (the RAR5 spec's
/// rule for split files); the final part carries the whole-member CRC32. That
/// is exactly what D4's two layers read.
fn single_member_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 1);
    let member_crc = checksum::crc32(payload);
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
                checksum::crc32(part)
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

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// Two articles per volume, so a volume's payload arrives after its header and
/// routing has to split at least one article across destinations.
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
            .map(|(index, (filename, bytes))| {
                let split = bytes.len().div_ceil(2) as u32;
                let rest = bytes.len() as u32 - split;
                FileSpec {
                    filename: filename.clone(),
                    role: FileRole::from_filename(filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![
                        segment_spec! {
                            number: 0,
                            bytes: yenc_declared_bytes(split),
                            message_id: format!("direct-{index}-0@example.com"),
                        },
                        segment_spec! {
                            number: 1,
                            bytes: yenc_declared_bytes(rest),
                            message_id: format!("direct-{index}-1@example.com"),
                        },
                    ],
                }
            })
            .collect(),
    }
}

/// Article arrival plan: `(file index, segment number)` in submission order.
fn in_order_arrivals(volume_count: usize) -> Vec<(u32, u32)> {
    (0..volume_count as u32)
        .flat_map(|file_index| [(file_index, 0), (file_index, 1)])
        .collect()
}

async fn submit_volume_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
) {
    let (filename, bytes) = &volumes[file_index as usize];
    let split = bytes.len().div_ceil(2);
    let (offset, payload) = if segment_number == 0 {
        (0u64, &bytes[..split])
    } else {
        (split as u64, &bytes[split..])
    };
    submit_decoded_segment(
        pipeline,
        NzbFileId { job_id, file_index },
        segment_number,
        offset,
        payload,
        filename,
        None,
    )
    .await;
}

/// Runs one whole job gate and returns `(final member bytes, status, whether a
/// source volume ever existed on disk)`.
async fn run_direct_store_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> (Option<Vec<u8>>, Option<JobStatus>, bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(false);

    let spec = direct_store_job_spec("Silver Horizon", volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
        for (filename, _) in volumes {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let status = job_status_for_assert(&pipeline, job_id);
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let member = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| std::fs::read(working_dir.join(member_name)).ok());
    (member, status, volume_file_seen)
}

/// Runs one job gate and returns the direct sets' final debug shape, without
/// driving extraction to a terminal state. Used where the point is what the
/// router decided, not what the job finished as.
async fn run_direct_store_routing_only(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> (String, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);

    let spec = direct_store_job_spec("Silver Horizon", volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    (shape, working_dir)
}

#[tokio::test]
async fn direct_store_output_is_byte_identical_to_the_conventional_extractor() {
    let member_name = "Silver.Horizon.S01E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 4);
    let arrivals = in_order_arrivals(volumes.len());

    let (conventional, conventional_status, conventional_volume_files) = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41001),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let (direct, direct_status, direct_volume_files) = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41002),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    // Non-vacuity: the conventional gate really did materialize volumes, so
    // "no volume file" below is a property of routing, not of the harness.
    assert!(
        conventional_volume_files,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct_volume_files,
        "direct routing must never create a source volume file"
    );

    assert_eq!(
        conventional.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert_eq!(
        direct, conventional,
        "direct-store output must be byte-identical to the conventional extractor"
    );
    assert_eq!(direct_status, conventional_status);
}

#[tokio::test]
async fn direct_store_routes_payload_that_lands_before_its_volume_header() {
    let member_name = "Silver.Horizon.S01E02.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Every volume's second article (pure payload) first, then the headers.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));

    let (direct, status, volume_files) = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41003),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!volume_files);
    assert_eq!(
        direct.as_deref(),
        Some(payload.as_slice()),
        "held bytes must drain to their destination once the header resolves"
    );
    assert!(matches!(status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn direct_store_ignores_a_duplicate_article() {
    let member_name = "Silver.Horizon.S01E03.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let mut arrivals = in_order_arrivals(volumes.len());
    // Re-deliver every article a second time.
    arrivals.extend(in_order_arrivals(volumes.len()));

    let (direct, status, volume_files) = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41004),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!volume_files);
    assert_eq!(direct.as_deref(), Some(payload.as_slice()));
    assert!(matches!(status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn direct_store_demotes_and_still_completes_when_the_member_checksum_is_wrong() {
    let member_name = "Silver.Horizon.S01E04.mkv";
    let payload: Vec<u8> = (0..2000u32).map(|index| (index % 173) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 3);

    // Corrupt the final part's payload while leaving its packed layer alone:
    // the yEnc layer is regenerated per article by the harness, so only the RAR
    // whole-member gate can catch this.
    let last = volumes.len() - 1;
    let length = volumes[last].1.len();
    volumes[last].1[length - 9] ^= 0xFF;

    let arrivals = in_order_arrivals(volumes.len());
    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41005), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)"),
        "the whole-member gate should have demoted the set, got {shape}"
    );
    assert!(
        !working_dir.join(member_name).exists(),
        "a member failing its whole-member gate must not be committed as if it passed"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists(),
        "demotion must delete the set's partial direct output"
    );
}
