//! Direct-store routing, phase 4 (plan 135).
//!
//! The spine is differential: the identical job gate is run with routing on and
//! off, and the outputs must be byte-identical. With routing on, no source
//! volume may ever appear on disk.

use super::*;

use crate::pipeline::direct_store::DirectStoreGate;
use crate::pipeline::direct_store::router::DemotionReason;

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

/// What one whole job gate produced.
#[derive(Debug, PartialEq, Eq)]
struct GateOutcome {
    member: Option<Vec<u8>>,
    /// Which of the two candidate directories the member landed in. Recorded
    /// rather than searched: "the file is in one of these places" would pass
    /// even if routing and the conventional extractor disagreed about where a
    /// finished member belongs.
    member_location: Option<&'static str>,
    status: Option<JobStatus>,
    volume_file_seen: bool,
}

async fn run_direct_store_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> GateOutcome {
    run_direct_store_gate_with_budget(gate, None, job_id, member_name, volumes, arrivals).await
}

async fn run_direct_store_gate_with_budget(
    gate: DirectStoreGate,
    holds_budget: Option<u64>,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> GateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    if let Some(bytes) = holds_budget {
        pipeline.direct_store.set_holds_budget(bytes);
    }
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
    let completed = std::fs::read(output_root.join(member_name)).ok();
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    assert!(
        completed.is_none() || left_behind.is_none(),
        "a finished member must exist in exactly one place"
    );
    let (member, member_location) = match (completed, left_behind) {
        (Some(bytes), _) => (Some(bytes), Some("complete")),
        (None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None) => (None, None),
    };
    GateOutcome {
        member,
        member_location,
        status,
        volume_file_seen,
    }
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

    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41001),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_direct_store_gate(
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
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "direct routing must never create a source volume file"
    );

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert_eq!(
        conventional.member_location,
        Some("complete"),
        "the conventional extractor moves a finished member to the complete directory"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "direct-store output must be byte-identical to the conventional extractor, \
         in the same directory, with the same job status"
    );
}

#[tokio::test]
async fn direct_store_routes_payload_that_lands_before_its_volume_header() {
    let member_name = "Silver.Horizon.S01E02.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Every volume's second article (pure payload) first, then the headers.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41003),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "held bytes must drain to their destination once the header resolves"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn direct_store_ignores_a_duplicate_article() {
    let member_name = "Silver.Horizon.S01E03.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let mut arrivals = in_order_arrivals(volumes.len());
    // Re-deliver every article a second time.
    arrivals.extend(in_order_arrivals(volumes.len()));

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41004),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(direct.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
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

// ---------------------------------------------------------------------------
// The confirming parse (B1)
// ---------------------------------------------------------------------------

/// A store set whose **last** volume carries a second, small member.
///
/// The second member's file header sits past the first member's data area, so
/// the last volume's *first* article never reaches it: a header walk over that
/// truncated prefix sees the first member's chain close and would conclude the
/// volume holds nothing else. Everything after that point is filed as envelope
/// and deleted at finalization — one whole file, silently lost.
fn store_set_with_a_member_hidden_past_the_first(
    member_name: &str,
    payload: &[u8],
    tail_name: &str,
    tail: &[u8],
) -> Vec<(String, Vec<u8>)> {
    let split = payload.len() / 2;
    let (first, second) = payload.split_at(split);

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0010,
        first.len() as u64,
        payload.len() as u64,
        Some(checksum::crc32(first)),
    ));
    part01.extend_from_slice(first);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0008,
        second.len() as u64,
        payload.len() as u64,
        Some(checksum::crc32(payload)),
    ));
    part02.extend_from_slice(second);
    part02.extend_from_slice(&build_test_rar_file_header(
        tail_name,
        0,
        tail.len() as u64,
        tail.len() as u64,
        Some(checksum::crc32(tail)),
    ));
    part02.extend_from_slice(tail);
    part02.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("silver.horizon.part01.rar".to_string(), part01),
        ("silver.horizon.part02.rar".to_string(), part02),
    ]
}

#[tokio::test]
async fn direct_store_refuses_a_set_whose_last_volume_hides_a_second_member() {
    let member_name = "Silver.Horizon.S01E07.mkv";
    let tail_name = "Silver.Horizon.nfo";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let tail = b"invented release notes for an invented release".to_vec();
    let volumes =
        store_set_with_a_member_hidden_past_the_first(member_name, &payload, tail_name, &tail);
    let arrivals = in_order_arrivals(volumes.len());

    // Non-vacuity: the hidden header really is out of reach of the last
    // volume's first article, so the provisional parse cannot see it.
    let last = &volumes[1].1;
    let first_article_end = last.len().div_ceil(2);
    let hidden_header_at = last
        .windows(tail_name.len())
        .position(|window| window == tail_name.as_bytes())
        .expect("the fixture must contain the second member's header");
    assert!(
        hidden_header_at > first_article_end,
        "the second member's header must land in the last volume's second article"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41010);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
    }

    // The confirming parse must have caught the extra member. Either demotion
    // reason is correct — the re-add conflicts, and the layout it conflicts
    // with is one the set may not route anyway.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(ConflictingVolumeFacts)")
            || shape.contains("Demoted(MultipleMembers)"),
        "a second member past the first member's data area must demote the set, got {shape}"
    );

    // And the job still finishes with *both* files: the demoted volumes are
    // refetched conventionally and the ordinary extractor produces the set.
    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let read_member = |name: &str| {
        std::fs::read(output_root.join(name))
            .ok()
            .or_else(|| std::fs::read(working_dir.join(name)).ok())
    };
    assert_eq!(
        read_member(member_name).as_deref(),
        Some(payload.as_slice()),
        "the first member must survive the demotion"
    );
    assert_eq!(
        read_member(tail_name).as_deref(),
        Some(tail.as_slice()),
        "the member hiding past the first one's data area must not be lost"
    );
}

// ---------------------------------------------------------------------------
// PAR2 admission refusal (B2)
// ---------------------------------------------------------------------------

/// The same spec with one PAR2 recovery file appended. Recovery volumes are not
/// data files, so the job still completes without them being delivered.
fn with_par2_recovery_file(mut spec: JobSpec) -> JobSpec {
    let filename = "silver.horizon.vol000+01.par2".to_string();
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&filename),
        filename,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: 4096,
            message_id: "direct-par2-0@example.com".to_string(),
        }],
    });
    spec
}

async fn run_par2_bearing_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) -> (Vec<Option<Vec<u8>>>, bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(false);

    let spec = with_par2_recovery_file(direct_store_job_spec("Silver Horizon", volumes));
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }

    let admitted = !pipeline.direct_store.sets_for(job_id).is_empty();
    let files = volumes
        .iter()
        .map(|(filename, _)| std::fs::read(working_dir.join(filename)).ok())
        .collect();
    (files, admitted)
}

#[tokio::test]
async fn a_par2_bearing_job_is_refused_at_admission_and_stays_conventional() {
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let (with_gate, admitted) =
        run_par2_bearing_gate(DirectStoreGate::Enabled, JobId(41011), &volumes).await;
    let (without_gate, _) =
        run_par2_bearing_gate(DirectStoreGate::Disabled, JobId(41012), &volumes).await;

    assert!(
        !admitted,
        "every PAR2 repair path reads the volume files direct routing never creates, \
         so a par2-bearing job may not admit a set at all"
    );
    for (index, (routed, conventional)) in with_gate.iter().zip(&without_gate).enumerate() {
        assert!(
            conventional.is_some(),
            "the conventional gate should have written volume {index}"
        );
        assert_eq!(
            routed, conventional,
            "with the gate on, a par2-bearing job must materialize volume {index} identically"
        );
    }
}

// ---------------------------------------------------------------------------
// Destinations, finalization and demotion accounting (B3/B4/B5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_routes_a_member_stored_inside_a_directory() {
    // The partial lives beside the member's eventual destination, so the first
    // routed byte opens a path whose parent directory nothing else creates.
    let member_name = "Silver.Horizon/S01E06.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 241) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41013),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "a member inside a directory must route and finalize like any other"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn a_duplicate_article_after_finalization_leaves_the_finished_output_alone() {
    let member_name = "Silver.Horizon.S01E09.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 137) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41014);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set should have finalized before the duplicate arrives, got {shape}"
    );
    let member_path = crate::pipeline::Pipeline::member_output_paths(&working_dir, member_name).0;
    let before = std::fs::read(&member_path).expect("the member is committed at finalization");
    assert_eq!(before, payload);

    // A late duplicate has nowhere to go: the partial it would be routed into
    // has already been renamed, and writing it conventionally would create the
    // source volume the whole design exists to avoid.
    submit_volume_article(&mut pipeline, job_id, &volumes, 1, 1).await;

    assert_eq!(
        std::fs::read(&member_path).ok().as_deref(),
        Some(payload.as_slice()),
        "a duplicate after finalization must not disturb the committed member"
    );
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "a duplicate after finalization must not materialize {filename}"
        );
    }
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists(),
        "a duplicate after finalization must not recreate the partial"
    );
}

/// Removes one segment from the job's queue, standing in for the dispatch that
/// pops it in the real pipeline. Without this the harness's queue still holds
/// every article, and "already queued" would cover everything.
fn take_queued_segment(pipeline: &mut Pipeline, job_id: JobId, segment_id: SegmentId) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let kept: Vec<_> = state
        .download_queue
        .drain_all()
        .into_iter()
        .filter(|work| work.segment_id != segment_id)
        .collect();
    let removed = kept.len();
    for work in kept {
        state.download_queue.push(work);
    }
    assert!(
        removed < state.download_queue.len() + 1,
        "the segment must have been queued before it is dispatched"
    );
}

fn queued_segments(pipeline: &mut Pipeline, job_id: JobId) -> Vec<(u32, u32)> {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let work = state.download_queue.drain_all();
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for item in work {
        assert!(
            seen.insert(item.segment_id),
            "{:?} is queued twice — the same article would be fetched from the server twice",
            item.segment_id
        );
        out.push((
            item.segment_id.file_id.file_index,
            item.segment_id.segment_number,
        ));
    }
    out.sort_unstable();
    out
}

#[tokio::test]
async fn a_demoted_set_refetches_only_what_nothing_else_owns() {
    let member_name = "Silver.Horizon.S01E10.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41015);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // A job-wide counter seeded by a *different* file's bytes, so "subtract the
    // set's contribution" is distinguishable from "zero the counter".
    const OTHER_FILE_BYTES: u64 = 7_000_000;
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes += OTHER_FILE_BYTES;

    // Volume 0 arrives and routes; volumes 1 and 2 stay queued, exactly as
    // they would mid-download.
    for segment_number in [0, 1] {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment_number).await;
    }
    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0
            })
            .unwrap()
            .received_bytes(),
        volumes[0].1.len() as u64,
        "volume 0's bytes were routed before the demotion"
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::MultipleMembers, None)
        .await;

    // Volume 0's articles come back because their bytes went into destinations
    // that were just deleted. Volumes 1 and 2 are already queued, and pushing
    // a second copy of each would download them twice.
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)],
        "every article is queued exactly once after a demotion"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        OTHER_FILE_BYTES,
        "the job counter loses the set's routed bytes and keeps every other file's"
    );
}

// ---------------------------------------------------------------------------
// Holds budget, and the demotion round trip it makes cheap to reach
// ---------------------------------------------------------------------------

/// Every volume's payload article before any of its headers, so nothing has a
/// destination and the whole set piles up as holds.
fn payload_before_header_arrivals(volume_count: usize) -> Vec<(u32, u32)> {
    let mut arrivals: Vec<(u32, u32)> = (0..volume_count as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volume_count as u32).map(|index| (index, 0)));
    arrivals
}

#[tokio::test]
async fn direct_store_demotes_when_held_bytes_exceed_their_budget() {
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Phase 4 has no scratch paging, so the ceiling is a demotion trigger
    // rather than a back-pressure signal; 64 MiB is unreachable in a test.
    pipeline.direct_store.set_holds_budget(64);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41018);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsBudgetExceeded)"),
        "payload with no destination and no room to hold it must demote, got {shape}"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists()
    );
}

#[tokio::test]
async fn a_demoted_set_completes_byte_identically_to_the_conventional_gate() {
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Payload first, with a holds ceiling too small to absorb it: the set
    // demotes on its very first article and the archive itself stays perfectly
    // valid, so the refetch has to reproduce the conventional result exactly.
    // The trailing arrival is that first article coming back — exactly what the
    // demotion re-queues, and the only article it re-queues.
    let mut arrivals = payload_before_header_arrivals(volumes.len());
    arrivals.push((0, 1));

    let direct = run_direct_store_gate_with_budget(
        DirectStoreGate::Enabled,
        Some(64),
        JobId(41019),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate_with_budget(
        DirectStoreGate::Disabled,
        None,
        JobId(41020),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate must produce the member"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a demoted set must finish exactly as the conventional path would have"
    );
    assert!(
        direct.volume_file_seen,
        "a demoted set's volumes are materialized by the conventional path"
    );
}

// ---------------------------------------------------------------------------
// Chain-close eligibility (revision 6 amendment 1)
// ---------------------------------------------------------------------------

/// A store set whose closing header states a BLAKE2sp digest and no CRC32.
///
/// Every earlier part carries a packed CRC32, so the member is provisionally
/// routable and its bytes really are placed; only when the chain closes does it
/// resolve ineligible, because BLAKE2sp accepts bytes in order only.
fn blake2_only_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
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

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            let extra = if is_last {
                build_test_rar_blake2_extra([0x42; 32])
            } else {
                Vec::new()
            };
            bytes.extend_from_slice(&build_test_rar_file_header_with_extra(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                (!is_last).then(|| checksum::crc32(part)),
                &extra,
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn direct_store_demotes_when_the_chain_closes_with_a_blake2_only_member() {
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = blake2_only_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41021), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "a member that resolves blake2-only when its chain closes must demote the set, got {shape}"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists(),
        "the demotion deletes the bytes routed while the member was provisional"
    );
}

// ---------------------------------------------------------------------------
// The yEnc whole-volume gate (M4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_demotes_a_volume_whose_yenc_whole_file_crc_disagrees() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41022);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Volume 0's articles are internally consistent — each one's own yEnc part
    // CRC is right — but the trailer they declare for the whole file is not the
    // CRC of the bytes they carry. A physical volume fails this at
    // file-complete time; a direct one has no file to re-read, so the check has
    // to be composed from the parts.
    //
    // Volume 0 declares a trailer that is not the CRC of the bytes it carries;
    // volume 1 declares the right one, so the same gate has to let it through —
    // a check that only ever fires is not a check.
    let honest = checksum::crc32(&volumes[1].1);
    let wrong = checksum::crc32(&volumes[0].1) ^ 0xFFFF_FFFF;
    for (file_index, declared) in [(1u32, honest), (0, wrong)] {
        for segment_number in [0, 1] {
            let (filename, bytes) = &volumes[file_index as usize];
            let split = bytes.len().div_ceil(2);
            let (offset, part) = if segment_number == 0 {
                (0u64, &bytes[..split])
            } else {
                (split as u64, &bytes[split..])
            };
            submit_decoded_segment(
                &mut pipeline,
                NzbFileId { job_id, file_index },
                segment_number,
                offset,
                part,
                filename,
                Some(declared),
            )
            .await;
        }
        if file_index == 1 {
            let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
            assert!(
                !shape.contains("Demoted"),
                "a volume whose composed CRC32 matches its trailer must pass the gate, got {shape}"
            );
        }
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(VolumeCrcMismatch)"),
        "a volume whose composed yEnc CRC32 disagrees with its trailer must demote \
         at volume completion, long before any member gate could run, got {shape}"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists()
    );
}

// ---------------------------------------------------------------------------
// D7 suppression and runtime lifetime (H3/H5)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_complete_direct_volume_never_refreshes_archive_state() {
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41023);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    assert_eq!(
        pipeline.try_update_archive_topology_calls, 0,
        "the routing seam must not re-probe a volume that has no file"
    );

    // The refresh has nine callers; these two are the completion check's
    // (check.rs) and RAR finalization's (rar.rs), which fire for any complete
    // file whether or not routing suppressed its own call.
    for file_index in 0..volumes.len() as u32 {
        let file_id = NzbFileId { job_id, file_index };
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, false)
            .await;
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, true)
            .await;
    }
    pipeline.try_rar_extraction(job_id).await;

    assert_eq!(
        pipeline.try_update_archive_topology_calls, 0,
        "an indirect caller must not re-probe a direct volume either"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "a direct set never enters the topology, so extraction is never dispatched for it"
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
}

#[tokio::test]
async fn removing_a_job_prunes_the_direct_store_runtime() {
    let member_name = "Silver.Horizon.S01E16.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 113) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41024);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the job admitted a set"
    );
    assert!(!pipeline.direct_store.is_empty_for(job_id));

    pipeline.purge_terminal_job_runtime(job_id);

    assert!(
        pipeline.direct_store.is_empty_for(job_id),
        "removing a job must leave no set, no examined mark and no prepared directory behind"
    );
    assert!(
        pipeline.direct_store.active_jobs().is_empty(),
        "a removed job's barriers must stop being polled"
    );
    // Idempotent, and safe on a job that never had a set.
    pipeline.purge_terminal_job_runtime(job_id);
    assert!(pipeline.direct_store.is_empty_for(job_id));
}

// ---------------------------------------------------------------------------
// Format detection (H1)
// ---------------------------------------------------------------------------

/// The RAR4 twin of [`single_member_store_set`].
///
/// RAR4 states the whole-member CRC32 in the *last* part's header and each
/// earlier part's own packed CRC32 in its own — the same two layers D4 reads,
/// in a completely different container.
fn single_member_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);

    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_main_header(is_first));
            bytes.extend_from_slice(&build_test_rar4_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn a_rar4_set_routes_directly_because_the_format_is_read_not_assumed() {
    // A hardcoded RAR5 layout fails `add_volume`'s format check on the very
    // first header a RAR4 set produces, so every RAR4 job would route nothing
    // and pay the whole demotion round trip. The format comes from the
    // signature the first volume actually carries.
    let member_name = "Silver.Horizon.S01E17.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 179) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );
    assert_ne!(
        &volumes[0].1[..8],
        &TEST_RAR5_SIG,
        "and it is not the RAR5 shape every other fixture here uses"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41025);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        for (filename, _) in &volumes {
            assert!(
                !working_dir.join(filename).exists(),
                "a RAR4 set must route without ever materializing {filename}"
            );
        }
    }

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    assert!(
        format!("{:?}", set.router).contains("format: Some(Rar4)"),
        "the router must bind the format the signature named, got {:?}",
        set.router
    );
    assert!(
        set.is_finalized(),
        "a RAR4 store set is as routable as a RAR5 one, got {set:?}"
    );
    assert_eq!(
        std::fs::read(crate::pipeline::Pipeline::member_output_paths(&working_dir, member_name).0)
            .ok()
            .as_deref(),
        Some(payload.as_slice()),
        "the routed RAR4 member must reproduce the payload byte for byte"
    );
}
