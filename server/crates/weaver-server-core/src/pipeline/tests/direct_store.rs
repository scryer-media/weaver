//! Direct-store routing, phase 4 (plan 135).
//!
//! The spine is differential: the identical job gate is run with routing on and
//! off, and the outputs must be byte-identical. With routing on, no source
//! volume may ever appear on disk.

use super::*;

use std::path::Path;

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

/// The decoded extent of one article, for a volume cut into `articles` equal
/// pieces. At `articles == 2` this is the head/tail split every phase 4 fixture
/// uses.
fn article_extent(volume_len: usize, segment_number: u32, articles: usize) -> (usize, usize) {
    let chunk = volume_len.div_ceil(articles);
    let start = (segment_number as usize * chunk).min(volume_len);
    let end = ((segment_number as usize + 1) * chunk).min(volume_len);
    (start, end)
}

/// Two articles per volume, so a volume's payload arrives after its header and
/// routing has to split at least one article across destinations.
fn direct_store_job_spec(name: &str, volumes: &[(String, Vec<u8>)]) -> JobSpec {
    direct_store_job_spec_with_articles(name, volumes, 2)
}

fn direct_store_job_spec_with_articles(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    articles: usize,
) -> JobSpec {
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
                segments: (0..articles as u32)
                    .map(|segment_number| {
                        let (start, end) = article_extent(bytes.len(), segment_number, articles);
                        segment_spec! {
                            number: segment_number,
                            bytes: yenc_declared_bytes((end - start) as u32),
                            message_id: format!("direct-{index}-{segment_number}@example.com"),
                        }
                    })
                    .collect(),
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
    submit_volume_article_of(pipeline, job_id, volumes, file_index, segment_number, 2).await;
}

async fn submit_volume_article_of(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
    articles: usize,
) {
    submit_volume_article_indexed_of(
        pipeline,
        job_id,
        volumes,
        file_index,
        file_index,
        segment_number,
        articles,
    )
    .await;
}

/// [`submit_volume_article`] for a set whose volumes are **not** NZB files
/// `0..n-1`: `ordinal` picks the bytes out of `volumes`, `file_index` is what
/// the job knows the file as. The two are the same number only when nothing
/// precedes the set in the NZB.
async fn submit_volume_article_indexed(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    ordinal: u32,
    file_index: u32,
    segment_number: u32,
) {
    submit_volume_article_indexed_of(
        pipeline,
        job_id,
        volumes,
        ordinal,
        file_index,
        segment_number,
        2,
    )
    .await;
}

async fn submit_volume_article_indexed_of(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    ordinal: u32,
    file_index: u32,
    segment_number: u32,
    articles: usize,
) {
    let (filename, bytes) = &volumes[ordinal as usize];
    let (start, end) = article_extent(bytes.len(), segment_number, articles);
    submit_decoded_segment(
        pipeline,
        NzbFileId { job_id, file_index },
        segment_number,
        start as u64,
        &bytes[start..end],
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
    run_direct_store_gate_with_ceilings(
        gate,
        holds_budget,
        None,
        job_id,
        member_name,
        volumes,
        arrivals,
    )
    .await
}

async fn run_direct_store_gate_with_ceilings(
    gate: DirectStoreGate,
    holds_budget: Option<u64>,
    scratch_ceiling: Option<u64>,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> GateOutcome {
    run_gate_with_password(
        gate,
        holds_budget,
        scratch_ceiling,
        job_id,
        member_name,
        volumes,
        arrivals,
        None,
    )
    .await
}

/// The gate runner with plan 136's one extra input: the job's password.
///
/// Everything else is the phase 4 harness unchanged, deliberately — the whole
/// point of the encrypted differentials is that turning the gate off with the
/// *same* password reproduces the same bytes, so both sides must run through
/// exactly the same code.
#[allow(clippy::too_many_arguments)]
async fn run_gate_with_password(
    gate: DirectStoreGate,
    holds_budget: Option<u64>,
    scratch_ceiling: Option<u64>,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    password: Option<&str>,
) -> GateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    if let Some(bytes) = holds_budget {
        pipeline.direct_store.set_holds_budget(bytes);
    }
    if let Some(bytes) = scratch_ceiling {
        pipeline.direct_store.set_holds_scratch_ceiling(bytes);
    }
    pipeline.live_par2.set_enabled(false);

    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = password.map(str::to_owned);
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

/// Phase 4's `direct_store_refuses_a_set_whose_last_volume_hides_a_second_member`,
/// upgraded: the hidden member is now **adopted** rather than demoting the set.
///
/// Phase 4 had two reasons to demote here — the layout refused the re-add, and
/// even if it had not, a second routable member was out of scope. Wave 1 removes
/// both: the router rebuilds its layout from every volume's newest facts when a
/// longer prefix reveals a header, and routes as many members as the archive has.
#[tokio::test]
async fn a_member_hiding_past_the_first_is_adopted_and_routes_direct() {
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
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
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

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the confirming parse must adopt the hidden member rather than demoting, got {shape}"
    );
    assert!(
        shape.contains("Finalized"),
        "both members pass their gates, so the set finalizes, got {shape}"
    );

    for (name, expected) in [
        (member_name, payload.as_slice()),
        (tail_name, tail.as_slice()),
    ] {
        assert_eq!(
            std::fs::read(crate::pipeline::Pipeline::member_output_paths(&working_dir, name).0)
                .ok()
                .as_deref(),
            Some(expected),
            "{name} must be committed byte for byte"
        );
    }
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "adopting a hidden member must not cost the set its volumes"
        );
    }
}

// ---------------------------------------------------------------------------
// PAR2 over virtual volumes (plan 135 phase 5 wave 2, D5)
// ---------------------------------------------------------------------------

/// Slice size for the PAR2 fixtures. Small enough that every volume carries
/// several slices — so a single damaged article shows up as damaged *slices*
/// rather than as one whole-file verdict — and large enough that the sets stay
/// quick to build.
const PAR2_SLICE_BYTES: u64 = 256;

/// A real PAR2 index over the set's **decoded volume bytes**.
///
/// The descriptions therefore name the volume files direct routing never
/// creates, which is exactly the shape the adapter has to answer for: file id,
/// length and every slice checksum are defined in source-volume space.
fn par2_index_over_volumes(volumes: &[(String, Vec<u8>)]) -> Vec<u8> {
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(filename, bytes)| (filename.as_str(), bytes.as_slice()))
        .collect();
    build_test_par2_index_for_files(&described, PAR2_SLICE_BYTES)
}

/// The set's spec plus a real, parseable PAR2 index file.
///
/// The index is a data file the pipeline downloads and parses like any other,
/// so `par2_set` loads through the production path rather than being installed
/// into the runtime by hand.
fn par2_bearing_job_spec(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
) -> (JobSpec, u32) {
    let mut spec = direct_store_job_spec(name, volumes);
    let index_filename = "silver.horizon.par2".to_string();
    let file_index = spec.files.len() as u32;
    spec.total_bytes += u64::from(yenc_declared_bytes(par2_bytes.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&index_filename),
        filename: index_filename,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(par2_bytes.len() as u32),
            message_id: "direct-par2-index@example.com".to_string(),
        }],
    });
    (spec, file_index)
}

/// What one par2-bearing job gate produced.
#[derive(Debug)]
struct Par2GateOutcome {
    member: Option<Vec<u8>>,
    member_location: Option<&'static str>,
    status: Option<JobStatus>,
    volume_file_seen: bool,
    admitted: bool,
    full_verify_skips: u64,
    authoritative_verify_calls: usize,
    demotions: String,
}

/// Runs one whole par2-bearing job gate.
///
/// The PAR2 index arrives **after** every volume, which is both the realistic
/// posting order and the one that matters: at the moment the last volume
/// completes there is no parsed PAR2 set yet, so a set that finalized on its own
/// gates would have deleted the volume image the verifier is about to need.
async fn run_par2_direct_gate(
    gate: DirectStoreGate,
    live_par2: bool,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
) -> Par2GateOutcome {
    let par2_bytes = par2_index_over_volumes(volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(live_par2);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
        for (filename, _) in volumes {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    let admitted = !pipeline.direct_store.sets_for(job_id).is_empty();

    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    for (filename, _) in volumes {
        if working_dir.join(filename).exists() {
            volume_file_seen = true;
        }
    }

    // Snapshotted here, not at the end: the PAR2 index completing is what runs
    // the verification, and a job that then completes has its direct-store
    // runtime pruned, so the sets are gone by the time extraction is terminal.
    let sets_after_verification = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let volume_file_at_end = volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());
    let volume_file_seen = volume_file_seen || volume_file_at_end;

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let completed = std::fs::read(output_root.join(member_name)).ok();
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    let (member, member_location) = match (completed, left_behind) {
        (Some(bytes), _) => (Some(bytes), Some("complete")),
        (None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None) => (None, None),
    };
    Par2GateOutcome {
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
        volume_file_seen,
        admitted,
        full_verify_skips: pipeline.live_par2.metrics().full_verify_skips,
        authoritative_verify_calls: pipeline.par2_authoritative_verify_calls,
        demotions: sets_after_verification,
    }
}

#[tokio::test]
async fn a_par2_bearing_direct_job_completes_byte_identically_and_never_writes_a_volume() {
    // Wave 1 refused this job at admission (`direct_store.refused.par2_present`)
    // because every PAR2 path read the volume files routing never creates. Wave
    // 2's `FileAccess` adapter answers those reads out of the envelope plus the
    // routed member bytes, so the refusal is gone and this test states the new
    // behaviour: the set routes, the job verifies against *virtual* volumes, and
    // the output is what the conventional extractor would have produced.
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let conventional = run_par2_direct_gate(
        DirectStoreGate::Disabled,
        true,
        JobId(41011),
        member_name,
        &volumes,
    )
    .await;
    let direct = run_par2_direct_gate(
        DirectStoreGate::Enabled,
        true,
        JobId(41012),
        member_name,
        &volumes,
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing job must admit its sets now that the verifier can read them virtually"
    );
    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a par2-bearing direct job must never create a source volume file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert_eq!(
        (
            direct.member.as_deref(),
            direct.member_location,
            &direct.status
        ),
        (
            conventional.member.as_deref(),
            conventional.member_location,
            &conventional.status
        ),
        "a par2-bearing direct job must produce the conventional gate's output, \
         in the same place, with the same status; sets = {}",
        direct.demotions
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?} with sets {}",
        direct.status,
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set should have finalized once verification cleared it, got {}",
        direct.demotions
    );
    // The point of D5: verification finishes with the download. Either the live
    // short-circuit fired, or the authoritative pass ran and did so entirely
    // against virtual volumes — both are wave-2 behaviour, and a job that did
    // neither would have failed the byte comparison above against zero files.
    assert!(
        direct.full_verify_skips > 0 || direct.authoritative_verify_calls > 0,
        "the job must have reached a PAR2 verdict; skips={} authoritative={}",
        direct.full_verify_skips,
        direct.authoritative_verify_calls
    );
}

/// Corrupts one byte of a volume's **recovery-record data area** — envelope
/// bytes that belong to no member and to no header.
///
/// The placement is the whole point, and it is the only placement that isolates
/// PAR2 as the detector:
///
/// - the yEnc layer is regenerated per article by the harness, so the transport
///   gate passes;
/// - the byte is outside every member's packed range, so neither the per-part
///   packed CRC32 nor the whole-member CRC32 covers it;
/// - it is inside a service block's *data*, not a header, so the header walk
///   still parses and the volume still confirms — damaging a header instead
///   would stop the walk and demote the set for a different reason entirely.
///
/// PAR2 covers the volume image, so PAR2 is the only layer left that can see it.
fn damage_recovery_record(volumes: &mut [(String, Vec<u8>)], volume: usize, rr_bytes: usize) {
    let offset = find_recovery_offset(&volumes[volume].1, rr_bytes);
    volumes[volume].1[offset + rr_bytes / 2] ^= 0xFF;
}

/// What one damaged par2-bearing gate ended up with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DamagedGateOutcome {
    status: Option<JobStatus>,
    member: Option<Vec<u8>>,
    /// Every volume's bytes as they finally sit in the working directory. For
    /// the conventional gate these are what the articles delivered; for the
    /// direct gate they are what demotion reconstructed, and the two must agree
    /// byte for byte or the reconstruction fabricated something.
    volume_files: Vec<Option<Vec<u8>>>,
    /// Whether the gate re-armed its own completion check on the way out of the
    /// demotion, sampled before anything else drives the job (M5). Without it
    /// the job's next move waits on the 30 s reconcile sweep.
    rearmed_after_demotion: bool,
}

async fn run_damaged_par2_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
) -> (DamagedGateOutcome, String) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    let sets_after_verification = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    // Sampled here, before anything else drives the job: the gate has just
    // returned from its demotion, and whether it queued its own next check is
    // the difference between "one loop turn" and "the 30 s reconcile sweep".
    let rearmed_after_demotion = pipeline.pending_completion_checks.contains(&job_id);
    // Snapshotted before the job reaches a terminal state, because a failed job
    // takes its working directory with it. This is the interesting moment
    // anyway: the direct gate's volumes here are what *demotion reconstructed*,
    // and the conventional gate's are what the articles delivered.
    let volume_files: Vec<Option<Vec<u8>>> = volumes
        .iter()
        .map(|(filename, _)| std::fs::read(working_dir.join(filename)).ok())
        .collect();

    // The harness delivers articles without dequeuing them, so the download
    // pipeline never looks exhausted and the repair gate waits forever for
    // targeted recovery that is not coming. Draining the queues is what lets the
    // gate reach its verdict, and it is what both gates get.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    for _ in 0..24 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let outcome = DamagedGateOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        member: std::fs::read(output_root.join(member_name))
            .ok()
            .or_else(|| std::fs::read(working_dir.join(member_name)).ok()),
        volume_files,
        rearmed_after_demotion,
    };
    (outcome, sets_after_verification)
}

#[tokio::test]
async fn par2_damage_a_direct_set_cannot_see_demotes_it_and_ends_where_the_conventional_gate_does()
{
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    // The PAR2 set describes the *clean* volumes; the job downloads damaged
    // ones. Building the index first is what makes the damage detectable.
    let par2_bytes = par2_index_over_volumes(&clean);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let (conventional, conventional_sets) = run_damaged_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41031),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;
    let (direct, direct_sets) = run_damaged_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41032),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert!(
        conventional_sets == "[]",
        "the conventional gate should never have admitted a set, got {conventional_sets}"
    );
    assert!(
        direct_sets.contains("Demoted(Par2Damaged)"),
        "PAR2 verification must catch damage the RAR and yEnc gates cannot see, on a \
         volume that only ever existed virtually, and demote — repairing a virtual \
         volume is phase 6 — got {direct_sets}"
    );
    assert!(
        direct.rearmed_after_demotion,
        "the demotion must re-arm the completion check on its way out (M5): the \
         materialized volumes are already on disk, and leaving the job's next move \
         to the 30 s reconcile sweep is a stall, not a wait"
    );
    // Non-vacuity, two ways. First: the same fixture *without* the damaged byte
    // runs clean through this very harness, so the outcome below is caused by
    // the damage rather than by the harness.
    let (clean_direct, clean_sets) = run_damaged_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41033),
        member_name,
        &clean,
        &par2_bytes,
    )
    .await;
    assert!(
        matches!(clean_direct.status, Some(JobStatus::Complete)),
        "the undamaged fixture must complete through the same harness, got {:?} with \
         sets {clean_sets}",
        clean_direct.status
    );
    assert!(
        !clean_sets.contains("Demoted"),
        "an undamaged par2-bearing direct set must never demote, got {clean_sets}"
    );
    // Second: the conventional gate saw the same damage — the set is
    // unrepairable (the fixture's PAR2 carries no recovery blocks), so it does
    // not complete either.
    assert!(
        !matches!(conventional.status, Some(JobStatus::Complete)),
        "the conventional gate should have been stopped by the same damage, got {:?}",
        conventional.status
    );
    assert!(
        direct.volume_files.iter().all(Option::is_some),
        "the demotion must materialize every volume for the conventional path to \
         repair, got {:?}",
        direct
            .volume_files
            .iter()
            .map(Option::is_some)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        direct.volume_files, conventional.volume_files,
        "the volumes demotion reconstructed from the set's own routed bytes must be \
         byte-identical to the ones the conventional gate downloaded — damage included, \
         since reconstruction rebuilds what arrived and does not judge it; \
         sets = {direct_sets}"
    );
    assert_eq!(
        direct.member, conventional.member,
        "neither gate may produce a member out of an unrepairable set; sets = {direct_sets}"
    );
    assert!(
        !matches!(direct.status, Some(JobStatus::Complete)),
        "the direct gate must not complete an unrepairable job, got {:?}",
        direct.status
    );
    // Job *status* is deliberately not compared. This fixture's PAR2 carries
    // descriptions and slice checksums but no recovery blocks — the test harness
    // has no builder for a parseable multi-file recovery stream — so neither gate
    // can repair, and the two reach that dead end through different waits (the
    // direct gate through its demotion, the conventional one through the
    // targeted-recovery wait). What wave 2 owns is the verdict and the bytes,
    // and both are asserted above; repair-to-success is phase 6's differential.
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

/// Drives one set to "volume 0 fully routed, volumes 1 and 2 still queued" and
/// then demotes it. Returns the pipeline so the caller can inspect the fallout.
async fn demote_mid_download(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    before_demotion: impl FnOnce(&Pipeline, &std::path::Path),
) -> (Pipeline, std::path::PathBuf, u64) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let spec = direct_store_job_spec("Silver Horizon", volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // A job-wide counter seeded by a *different* file's bytes, so "subtract the
    // set's contribution" is distinguishable from "zero the counter".
    const OTHER_FILE_BYTES: u64 = 7_000_000;
    pipeline.jobs.get_mut(&job_id).unwrap().downloaded_bytes += OTHER_FILE_BYTES;

    // Volume 0 arrives whole; volume 1 gets only its first article, so the set
    // demotes with one fully covered volume and one partially covered one.
    // Volume 2 never arrives at all.
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0)] {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
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

    before_demotion(&pipeline, &working_dir);
    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded, None)
        .await;
    (pipeline, working_dir, OTHER_FILE_BYTES)
}

#[tokio::test]
async fn a_demoted_set_materializes_its_covered_volumes_instead_of_refetching_them() {
    let member_name = "Silver.Horizon.S01E10.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41015);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _| {}).await;

    // D8: volume 0 was covered end to end, so it is rebuilt byte-exactly from
    // its envelope plus the member partial — the whole point of reconstruction
    // is that those two articles never touch the network again.
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the covered volume must be reconstructed byte for byte"
    );
    // Volume 1 is rebuilt only as far as its coverage reaches: the prefix its
    // first article carried, and not one byte of the article that never came.
    let volume_one_prefix = volumes[1].1.len().div_ceil(2);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[1].0))
            .ok()
            .as_deref(),
        Some(&volumes[1].1[..volume_one_prefix]),
        "a partially covered volume is rebuilt exactly as far as its coverage"
    );

    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(1, 1), (2, 0), (2, 1)],
        "a reconstructed article must not be fetched from the server again, and \
         everything above the floor must be"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volumes[0].1.len() as u64 + volume_one_prefix as u64,
        "bytes that survived as a real volume stay counted; nothing else does"
    );

    // The direct outputs are gone: a sparse half-written member would
    // masquerade as finished work, and the envelopes are scratch.
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists()
    );
    for volume_index in 0..volumes.len() as u32 {
        assert!(
            !working_dir
                .join(format!("silver.horizon.vol{volume_index:05}.envelope"))
                .exists(),
            "envelope {volume_index} must be deleted once the volume is real"
        );
    }

    // Reconciliation persisted legacy state in D8's order and shape: a whole
    // volume becomes a completed-file row, a partial one a contiguous floor,
    // and the direct coverage row is retired behind both.
    let (floors, complete) = pipeline.db.load_active_file_runtime(job_id).unwrap();
    assert!(
        complete.contains(&0),
        "a fully reconstructed volume is persisted as a completed file, got {complete:?}"
    );
    assert!(
        !complete.contains(&1),
        "a partially reconstructed volume must not be claimed complete"
    );
    assert_eq!(
        floors.get(&1).copied(),
        Some(volume_one_prefix as u64),
        "the partial volume persists a contiguous, segment-aligned floor"
    );
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "the direct coverage row is retired once the legacy state replaces it"
    );
}

#[tokio::test]
async fn a_demotion_falls_back_to_refetching_when_its_envelope_is_gone() {
    // Reconstruction is an optimisation; every one of its failure modes has to
    // land on phase 4's always-correct refetch rather than on a half-written
    // volume. Deleting the envelope out from under the sweep is the bluntest of
    // them: the header bytes it needs are simply not there any more.
    let member_name = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41031);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            let envelope = working_dir.join("silver.horizon.vol00000.envelope");
            assert!(envelope.exists(), "the envelope must exist to be deleted");
            std::fs::remove_file(&envelope).unwrap();
        })
        .await;

    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "a failed reconstruction must not leave a partly written volume behind"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)],
        "the fallback refetches every article exactly once"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes,
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

/// D2's paging, at the seam the RAM budget used to demote at.
///
/// The first article is pure payload with no header yet, so it has nowhere to go
/// and is held; the budget is far below it. Wave 3 pages it to the set's scratch
/// file instead of demoting, and the set stays live.
#[tokio::test]
async fn direct_store_pages_held_bytes_to_scratch_instead_of_demoting() {
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41018);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a holds breach must page, not demote, got {shape}"
    );
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(
        set.router.scratch_bytes() > 0,
        "the held payload must have reached the scratch file"
    );
    assert!(
        set.router.resident_staged_bytes() <= 64,
        "paging must bring RAM back inside the budget, got {}",
        set.router.resident_staged_bytes()
    );
    // Named from the set so the restart sweep can find it by prefix, and
    // disambiguated by the set's lowest NZB file index so two set names that
    // sanitize to one component never share a scratch file — and with it, an
    // append cursor and a region index.
    assert!(
        working_dir.join(".weaver-holds.silver.horizon.f0").exists(),
        "the scratch file is named from the set, with its disambiguator, so the restart \
         sweep can find it"
    );
    assert_eq!(
        set.router.unaccounted_staged_bytes(),
        0,
        "every staged byte is either RAM-resident or paged; neither ceiling may be blind to it"
    );
}

/// The paged holds are not merely stored — they route, and the set finishes
/// byte-identically to a run that never breached its budget.
#[tokio::test]
async fn a_set_that_paged_its_holds_still_one_passes_byte_identically() {
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 151) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Every volume's payload before any header, so every one of them is held.
    let arrivals = payload_before_header_arrivals(volumes.len());

    let paged = run_direct_store_gate_with_budget(
        DirectStoreGate::Enabled,
        Some(64),
        JobId(41051),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let unpaged = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41052),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41053),
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
    assert!(
        !paged.volume_file_seen,
        "paging holds must not materialize a source volume"
    );
    assert_eq!(
        (
            paged.member.as_ref(),
            paged.member_location,
            paged.status.clone()
        ),
        (
            unpaged.member.as_ref(),
            unpaged.member_location,
            unpaged.status
        ),
        "a paged run must match an unpaged direct run exactly"
    );
    assert_eq!(
        (paged.member, paged.member_location, paged.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a paged run must match the conventional extractor exactly"
    );
}

/// The scratch ceiling is the last lever: past it there is nowhere left to put
/// the holds, and the set demotes with its own reason rather than the RAM one.
#[tokio::test]
async fn a_scratch_ceiling_breach_demotes_the_set() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_scratch_ceiling(16);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41054);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchCeiling)"),
        "a scratch ceiling breach must demote with its own reason, got {shape}"
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

    // Payload first, with neither RAM nor scratch room to absorb it: the set
    // demotes on its very first article and the archive itself stays perfectly
    // valid, so the refetch has to reproduce the conventional result exactly.
    // The trailing arrival is that first article coming back — exactly what the
    // demotion re-queues, and the only article it re-queues.
    let mut arrivals = payload_before_header_arrivals(volumes.len());
    arrivals.push((0, 1));

    let direct = run_direct_store_gate_with_ceilings(
        DirectStoreGate::Enabled,
        Some(64),
        Some(16),
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

    // The label is not the outcome (B1). The demotion runs reconstruction, and
    // the member whose eligibility just flipped is the one holding most of every
    // volume's bytes: a sweep that asked the *current* classification where they
    // live would be told "the envelope", read a sparse hole, and write zeros
    // into a volume file under a published floor. Whatever the demotion decides
    // to do, what lands on disk has to be the volume.
    assert_volumes_are_never_fabricated(&working_dir, &volumes);
    for volume_index in [0usize, 1] {
        let (filename, bytes) = &volumes[volume_index];
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "volume {volume_index} was covered end to end and must be rebuilt byte for byte \
             from the partial its now-ineligible member still holds"
        );
    }
}

/// Every source volume of `volumes` that exists on disk is a byte-exact prefix
/// of the volume that was posted.
///
/// The assertion demotion has to satisfy however it goes: a reconstruction
/// writes the volume, a refetch fallback writes nothing, and neither may leave
/// bytes that were never downloaded looking like bytes that were.
fn assert_volumes_are_never_fabricated(
    working_dir: &std::path::Path,
    volumes: &[(String, Vec<u8>)],
) {
    for (filename, bytes) in volumes {
        let Ok(written) = std::fs::read(working_dir.join(filename)) else {
            continue;
        };
        assert!(
            written.len() <= bytes.len(),
            "{filename} was materialized longer than the volume it stands for"
        );
        assert_eq!(
            written.as_slice(),
            &bytes[..written.len()],
            "{filename} was materialized with bytes the set never downloaded"
        );
    }
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

// ---------------------------------------------------------------------------
// Multi-member sets (phase 5 wave 1)
// ---------------------------------------------------------------------------

/// A store set carrying several members, split across `volume_count` volumes.
///
/// The members are laid end to end and the concatenation is cut into equal
/// volume payloads, so member boundaries and volume boundaries deliberately do
/// **not** line up: members start mid-volume, at least one is split across
/// volumes, and one volume carries the tail of one member and the head of the
/// next. That is the shape a season pack posts as, and the shape phase 4
/// demoted on sight.
fn multi_member_store_set(
    members: &[(&str, Vec<u8>)],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 1);
    let total: usize = members.iter().map(|(_, bytes)| bytes.len()).sum();
    let chunk = total.div_ceil(volume_count);

    // Each member's span in the concatenated payload space.
    let mut spans = Vec::with_capacity(members.len());
    let mut cursor = 0usize;
    for (name, bytes) in members {
        spans.push((*name, bytes.as_slice(), cursor, cursor + bytes.len()));
        cursor += bytes.len();
    }

    (0..volume_count)
        .map(|volume| {
            let window_start = (volume * chunk).min(total);
            let window_end = ((volume + 1) * chunk).min(total);
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));

            for (name, payload, start, end) in &spans {
                let part_start = (*start).max(window_start);
                let part_end = (*end).min(window_end);
                if part_start >= part_end {
                    continue;
                }
                let part = &payload[part_start - start..part_end - start];
                let split_before = part_start > *start;
                let split_after = part_end < *end;

                let mut split_flags = 0u64;
                if split_before {
                    split_flags |= 0x0008;
                }
                if split_after {
                    split_flags |= 0x0010;
                }
                // The RAR5 rule D4 layer 1 reads: a non-final part states the
                // CRC32 of *its own* packed bytes, the final part the whole
                // member's.
                let data_crc = if split_after {
                    checksum::crc32(part)
                } else {
                    checksum::crc32(payload)
                };
                bytes.extend_from_slice(&build_test_rar_file_header(
                    name,
                    split_flags,
                    part.len() as u64,
                    payload.len() as u64,
                    Some(data_crc),
                ));
                bytes.extend_from_slice(part);
            }

            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// One member as a gate saw it: name, bytes, and which of the two candidate
/// directories it landed in.
type GateMember = (String, Option<Vec<u8>>, Option<&'static str>);

/// What one whole job gate produced for a multi-member set.
#[derive(Debug, PartialEq, Eq)]
struct MultiGateOutcome {
    /// One entry per requested member name, in the order asked for.
    members: Vec<GateMember>,
    status: Option<JobStatus>,
    volume_file_seen: bool,
}

async fn run_multi_member_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_names: &[&str],
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> MultiGateOutcome {
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

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let members = member_names
        .iter()
        .map(|name| {
            let completed = std::fs::read(output_root.join(name)).ok();
            let left_behind = std::fs::read(working_dir.join(name)).ok();
            assert!(
                completed.is_none() || left_behind.is_none(),
                "{name} must exist in exactly one place"
            );
            match (completed, left_behind) {
                (Some(bytes), _) => (name.to_string(), Some(bytes), Some("complete")),
                (None, Some(bytes)) => (name.to_string(), Some(bytes), Some("working")),
                (None, None) => (name.to_string(), None, None),
            }
        })
        .collect();
    MultiGateOutcome {
        members,
        status: job_status_for_assert(&pipeline, job_id),
        volume_file_seen,
    }
}

#[tokio::test]
async fn a_two_member_store_set_routes_and_matches_the_conventional_extractor() {
    let episode = "Silver.Horizon.S01E01.mkv";
    let notes = "Silver.Horizon.S01E01.nfo";
    let members = vec![
        (
            episode,
            (0..3400u32).map(|index| (index % 251) as u8).collect(),
        ),
        // A tiny member, so one volume carries the tail of the first and the
        // whole of the second.
        (notes, b"invented notes for an invented release".to_vec()),
    ];
    let volumes = multi_member_store_set(&members, 3);
    let arrivals = in_order_arrivals(volumes.len());
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41040),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41041),
        &names,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a multi-member set must route without materializing a volume"
    );
    for (name, bytes, _) in &conventional.members {
        let expected = members
            .iter()
            .find(|(member, _)| member == name)
            .map(|(_, payload)| payload.as_slice());
        assert_eq!(
            bytes.as_deref(),
            expected,
            "the conventional extractor should reproduce {name}"
        );
    }
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a two-member direct set must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_three_member_set_with_a_directory_member_matches_the_conventional_extractor() {
    let inside = "Silver.Horizon/S01E02.mkv";
    let episode = "Silver.Horizon.S01E03.mkv";
    let notes = "Silver.Horizon.nfo";
    let members = vec![
        // A member stored inside a directory: routing has to create the parent
        // before the first byte lands, where extraction creates it as it writes.
        (
            inside,
            (0..2600u32).map(|index| (index % 241) as u8).collect(),
        ),
        (
            episode,
            (0..3100u32).map(|index| (index % 193) as u8).collect(),
        ),
        (notes, b"an invented release, described briefly".to_vec()),
    ];
    let volumes = multi_member_store_set(&members, 4);
    let arrivals = in_order_arrivals(volumes.len());
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41042),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41043),
        &names,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(conventional.volume_file_seen);
    assert!(!direct.volume_file_seen);
    for (name, bytes, location) in &conventional.members {
        let expected = members
            .iter()
            .find(|(member, _)| member == name)
            .map(|(_, payload)| payload.as_slice());
        assert_eq!(
            bytes.as_deref(),
            expected,
            "the conventional extractor should reproduce {name}"
        );
        assert_eq!(*location, Some("complete"));
    }
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a three-member direct set must be byte-identical to the conventional extractor, \
         directory member included"
    );
}

#[tokio::test]
async fn two_members_that_sanitize_to_one_destination_demote_the_set() {
    // `ensure_unique_sanitized_rar_member_paths` refuses an archive whose
    // members collide after sanitization, so the extractor never overwrites one
    // with the other — and neither may direct routing, which is why the set
    // demotes and lets the ordinary path produce today's outcome. The commit
    // loop still walks members in archive order, which is what makes this
    // decidable at all rather than dependent on arrival order.
    let members = vec![
        (
            "./Silver.Horizon.nfo",
            b"the first of two names for one destination".to_vec(),
        ),
        (
            "Silver.Horizon.nfo",
            b"the second of two names for one destination".to_vec(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 2);
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41044), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(CollidingDestinations)"),
        "two members sanitizing to one path must demote rather than overwrite, got {shape}"
    );
    assert!(
        !working_dir.join("Silver.Horizon.nfo").exists(),
        "neither colliding member may be committed"
    );
}

// ---------------------------------------------------------------------------
// Envelope v2: recovery records route (phase 5 wave 1)
// ---------------------------------------------------------------------------

/// A store set whose volumes each carry a recovery record after the payload.
///
/// The RR is a service header plus a data area belonging to no member, so every
/// byte of it is envelope. At `rr_bytes` well over phase 4's 32 KiB half-slot
/// this set could not route at all before envelope v2 — it demoted with
/// `EnvelopeTooLarge`, which is why every `-rr` post did.
fn recovery_record_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    rr_bytes: usize,
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
            bytes.extend_from_slice(&build_test_rar_file_header(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                }),
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_service_header("RR", rr_bytes as u64));
            bytes.extend((0..rr_bytes).map(|index| ((index * 7 + volume * 13) % 256) as u8));
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn a_recovery_record_set_routes_direct_and_its_envelopes_carry_the_recovery_data() {
    const RR_BYTES: usize = 48 * 1024;
    let member_name = "Silver.Horizon.S01E18.mkv";
    let payload: Vec<u8> = (0..40_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let arrivals = in_order_arrivals(volumes.len());

    // Non-vacuity: the recovery record alone is bigger than the whole 64 KiB
    // slot phase 4 gave a volume, let alone the 32 KiB half it addressed the
    // head from. This set could not have routed a byte before envelope v2.
    const { assert!(RR_BYTES > 32 * 1024) };

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41045);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Halfway through, the envelope files must already hold the recovery data at
    // its true physical offset — the property that makes the whole scheme
    // restart-stable and unbounded.
    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
        if (*file_index, *segment_number) == (0, 1) {
            let envelope = working_dir.join("silver.horizon.vol00000.envelope");
            let written = std::fs::read(&envelope).expect("volume 0's envelope exists");
            let rr_at = find_recovery_offset(&volumes[0].1, RR_BYTES);
            assert!(
                written.len() >= rr_at + RR_BYTES,
                "the envelope must be long enough to hold the recovery record at its \
                 physical offset ({} < {})",
                written.len(),
                rr_at + RR_BYTES
            );
            assert_eq!(
                &written[rr_at..rr_at + RR_BYTES],
                &volumes[0].1[rr_at..rr_at + RR_BYTES],
                "the recovery record must land in the envelope byte for byte, at its \
                 true physical offset"
            );
        }
    }

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    assert!(
        set.is_finalized(),
        "an -rr set routes and finalizes under envelope v2, got {set:?}"
    );
    assert_eq!(
        std::fs::read(crate::pipeline::Pipeline::member_output_paths(&working_dir, member_name).0)
            .ok()
            .as_deref(),
        Some(payload.as_slice()),
        "the member must be byte-correct even with a recovery record between the parts"
    );
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
    for volume_index in 0..volumes.len() as u32 {
        assert!(
            !working_dir
                .join(format!("silver.horizon.vol{volume_index:05}.envelope"))
                .exists(),
            "finalization must delete envelope {volume_index}"
        );
    }
}

/// Physical offset of the recovery record's data area inside a fixture volume.
///
/// Found by construction rather than by scanning for a byte pattern: the RR data
/// is the last `rr_bytes` before the end-of-archive header, whose encoded length
/// the builder fixes.
fn find_recovery_offset(volume: &[u8], rr_bytes: usize) -> usize {
    let end_header = build_test_rar_end_header(true).len();
    volume.len() - end_header - rr_bytes
}

// ---------------------------------------------------------------------------
// The hybrid virtual-volume provider, differentially (phase 5 wave 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_virtual_volume_reads_back_the_volume_the_conventional_gate_would_have_written() {
    use std::io::Read;

    // The unit tests build a virtual volume by hand; this one builds it the way
    // production does — out of whatever routing happened to put where — and
    // holds it to the only standard that matters: byte-for-byte agreement with
    // the volume the conventional gate writes to disk.
    let members = vec![
        (
            "Silver.Horizon.S01E19.mkv",
            (0..3400u32).map(|index| (index % 251) as u8).collect(),
        ),
        (
            "Silver.Horizon.nfo",
            b"invented notes for an invented release".to_vec(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41046);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Volumes 0 and 1 whole, volume 2 only its first article — so the same
    // provider has to answer both "this volume is complete" and "this volume
    // stops here" without the caller telling it which is which.
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0), (1, 1), (2, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the set is still routing, got {shape}"
    );

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    let prefix = volumes[2].1.len().div_ceil(2);
    let lengths: std::collections::BTreeMap<u32, u64> = [
        (0u32, volumes[0].1.len() as u64),
        (1, volumes[1].1.len() as u64),
        (2, volumes[2].1.len() as u64),
    ]
    .into_iter()
    .collect();
    let provider = set.virtual_provider(&lengths);

    for volume_index in [0usize, 1] {
        let mut reader = provider
            .open(volume_index as u32)
            .expect("the volume is registered");
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).unwrap();
        assert_eq!(
            read_back, volumes[volume_index].1,
            "virtual volume {volume_index} must equal the source volume byte for byte"
        );
    }

    // The half-arrived volume reads its covered prefix and then reports a hole
    // rather than pretending the volume ends there.
    let mut reader = provider.open(2).unwrap();
    let mut read_back = vec![0u8; prefix];
    reader.read_exact(&mut read_back).unwrap();
    assert_eq!(
        read_back,
        volumes[2].1[..prefix],
        "a partly arrived volume still reads back exactly what did arrive"
    );
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        crate::pipeline::direct_store::provider::is_hole(&error),
        "the bytes that never arrived must read as a hole, got {error}"
    );
}

// ---------------------------------------------------------------------------
// The classification frontier (phase 5 wave 1)
// ---------------------------------------------------------------------------

/// A two-volume store set whose second volume carries a second member after the
/// first, with both members' payloads sized so that — cut into three articles —
/// the second member's *header* lands in the middle article and its *data*
/// spans the middle/last boundary.
///
/// That geometry is the whole point: with the middle article missing, the header
/// walk seeks to the end of the first member's data, finds a hole where the
/// second member's header should be, and stops. Everything the last article
/// carries is then a member's payload that the layout cannot name yet.
fn set_with_a_second_member_behind_a_header_hole(
    first_name: &str,
    first_payload: &[u8],
    second_name: &str,
    second_payload: &[u8],
) -> Vec<(String, Vec<u8>)> {
    let split = first_payload.len() / 2;
    let (head, tail) = first_payload.split_at(split);

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        first_name,
        0x0010,
        head.len() as u64,
        first_payload.len() as u64,
        Some(checksum::crc32(head)),
    ));
    part01.extend_from_slice(head);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        first_name,
        0x0008,
        tail.len() as u64,
        first_payload.len() as u64,
        Some(checksum::crc32(first_payload)),
    ));
    part02.extend_from_slice(tail);
    part02.extend_from_slice(&build_test_rar_file_header(
        second_name,
        0,
        second_payload.len() as u64,
        second_payload.len() as u64,
        Some(checksum::crc32(second_payload)),
    ));
    part02.extend_from_slice(second_payload);
    part02.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("silver.horizon.part01.rar".to_string(), part01),
        ("silver.horizon.part02.rar".to_string(), part02),
    ]
}

#[tokio::test]
async fn payload_past_the_last_known_header_is_held_until_the_walk_proves_what_it_is() {
    // Adopting a late member (the confirming parse) is only safe if its bytes
    // are still routable when it is adopted. A byte past the last member extent
    // the walk has *reached* has no proven classification: it can be an
    // envelope byte, or it can be an undiscovered member's payload. Filing it as
    // envelope on a guess loses a whole file, because finalization deletes the
    // envelopes.
    let episode = "Silver.Horizon.S01E21.mkv";
    let notes = "Silver.Horizon.S01E21.nfo";
    let episode_payload: Vec<u8> = (0..1200u32).map(|index| (index % 251) as u8).collect();
    let notes_payload: Vec<u8> = (0..600u32).map(|index| (index % 197) as u8).collect();
    let volumes = set_with_a_second_member_behind_a_header_hole(
        episode,
        &episode_payload,
        notes,
        &notes_payload,
    );

    // Non-vacuity: with the middle article absent, the second member's header
    // really is unreachable and its data really does reach into the last one.
    let last = &volumes[1].1;
    let header_at = last
        .windows(notes.len())
        .position(|window| window == notes.as_bytes())
        .expect("the fixture carries the second member's header");
    let (first_end, _) = article_extent(last.len(), 0, 3);
    let (middle_start, middle_end) = article_extent(last.len(), 1, 3);
    assert!(
        header_at >= middle_start && header_at < middle_end,
        "the second member's header must sit in the middle article ({header_at} not in \
         {middle_start}..{middle_end})"
    );
    assert!(
        last.len() - middle_end > 0 && first_end < middle_start,
        "its data must reach into the last article"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41047);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, 3);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Volume 0 whole, then volume 1's first and *last* articles — leaving the
    // header hole open — and only afterwards the middle one that closes it.
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (0, 2), (1, 0), (1, 2), (1, 1)] {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            3,
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must adopt the member behind the header hole and finalize, got {shape}"
    );
    for (name, expected) in [
        (episode, episode_payload.as_slice()),
        (notes, notes_payload.as_slice()),
    ] {
        assert_eq!(
            std::fs::read(crate::pipeline::Pipeline::member_output_paths(&working_dir, name).0)
                .ok()
                .as_deref(),
            Some(expected),
            "{name} must be byte-correct — its payload arrived before its header did"
        );
    }
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
}

// ---------------------------------------------------------------------------
// Demotion after a routed member turns ineligible (B1)
// ---------------------------------------------------------------------------

/// Three volumes, each carrying a recovery record, whose split member's chain
/// closes with a BLAKE2sp digest and no CRC32 (`-htb`), and whose last volume
/// hides a second member past the first's data area.
///
/// Every ingredient earns its place. The `-htb` close is the confirmed-reachable
/// transition that flips a member from `ProvisionallyDirect` to `Ineligible`
/// *after* its bytes have been routed. The recovery record makes each envelope
/// file long and sparse, so a read at the member's physical offsets succeeds and
/// returns zeros instead of failing — which is what makes the failure silent.
/// The hidden member is what a real multi-member store looks like at the moment
/// the demotion fires: one member routed, one the layout has not reached.
fn blake2_close_with_recovery_and_hidden_member(
    member_name: &str,
    payload: &[u8],
    hidden_name: &str,
    hidden_payload: &[u8],
    volume_count: usize,
    rr_bytes: usize,
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
                build_test_rar_blake2_extra([0x37; 32])
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
            if is_last {
                bytes.extend_from_slice(&build_test_rar_file_header(
                    hidden_name,
                    0,
                    hidden_payload.len() as u64,
                    hidden_payload.len() as u64,
                    Some(checksum::crc32(hidden_payload)),
                ));
                bytes.extend_from_slice(hidden_payload);
            }
            bytes.extend_from_slice(&build_test_rar_service_header("RR", rr_bytes as u64));
            bytes.extend((0..rr_bytes).map(|index| ((index * 11 + volume * 17) % 256) as u8));
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn a_member_that_turns_ineligible_after_routing_never_materializes_fabricated_bytes() {
    const RR_BYTES: usize = 4096;
    let member_name = "Silver.Horizon.S01E23.mkv";
    let hidden_name = "Silver.Horizon.S01E23.nfo";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let hidden_payload: Vec<u8> = (0..600u32).map(|index| (index % 197) as u8).collect();
    let volumes = blake2_close_with_recovery_and_hidden_member(
        member_name,
        &payload,
        hidden_name,
        &hidden_payload,
        3,
        RR_BYTES,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41051), &volumes, &arrivals).await;
    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "the chain closing blake2-only must demote the set, got {shape}"
    );

    // Non-vacuity: the envelope for a volume whose member data was routed away is
    // sparse across exactly those offsets, and long enough — the recovery record
    // sits past them — that a read there returns zeros rather than short. That is
    // the read a sweep taking its extents from the current classification would
    // have made.
    assert!(
        RR_BYTES > payload.len(),
        "the recovery record must extend the envelope well past the member's data"
    );

    assert_volumes_are_never_fabricated(&working_dir, &volumes);
    for volume_index in [0usize, 1] {
        let (filename, bytes) = &volumes[volume_index];
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "volume {volume_index} must come back byte for byte: its member bytes from the \
             partial the router wrote them to, its recovery record from the envelope"
        );
    }
    assert!(
        !working_dir.join(&volumes[2].0).exists(),
        "the volume the demotion fired on covered nothing, so it is refetched whole"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists(),
        "the direct outputs are deleted once the volumes are real"
    );
}

#[tokio::test]
async fn a_partially_covered_volume_is_verified_before_it_is_materialized() {
    // M3 plus B1(c): a volume covered only as far as its first article still has
    // a composed reference for that prefix, and the sweep checks it. Corrupting
    // the member partial under the covered prefix is what a partial that came
    // back wrong looks like — and it has to end in the refetch, not in a volume
    // file nothing verified.
    let member_name = "Silver.Horizon.S01E24.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Volume 1 carries the member's logical bytes from 800 onwards, and only its
    // first article was covered, so this offset is inside the *partially*
    // covered volume's run and outside volume 0's.
    const CORRUPTED_LOGICAL_OFFSET: u64 = 850;

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41052);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            use std::io::{Seek, SeekFrom, Write};
            let partial = working_dir.join(format!("{member_name}.direct.partial"));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&partial)
                .expect("the member partial holds the routed bytes");
            file.seek(SeekFrom::Start(CORRUPTED_LOGICAL_OFFSET))
                .unwrap();
            file.write_all(&[0xFF]).unwrap();
            file.sync_all().unwrap();
        })
        .await;

    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "a run that fails its composed CRC32 must leave no volume behind, not even \
             the volumes that verified"
        );
    }
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)],
        "the fallback refetches every article exactly once"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes,
        "nothing survived as a real volume, so nothing stays counted"
    );
}

// ---------------------------------------------------------------------------
// The staged-bytes budget (M1)
// ---------------------------------------------------------------------------

/// A two-volume set whose **last** volume carries a recovery record between the
/// split member's final part and a second, whole member.
///
/// The geometry is what makes the retained region observable: the second
/// member's data area ends far past the volume's first article, so the volume is
/// unconfirmed while that article is routed — and the recovery record, sitting
/// *below* the last known member extent, is classified envelope, written, and
/// then held in RAM for the header walk to seek through.
///
/// Returns the physical offset of the recovery record's data inside volume 1.
fn recovery_record_between_members_set(
    first_name: &str,
    first_payload: &[u8],
    second_name: &str,
    second_payload: &[u8],
    rr_bytes: usize,
) -> (Vec<(String, Vec<u8>)>, usize) {
    let split = first_payload.len() / 2;
    let (head, tail) = first_payload.split_at(split);

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        first_name,
        0x0010,
        head.len() as u64,
        first_payload.len() as u64,
        Some(checksum::crc32(head)),
    ));
    part01.extend_from_slice(head);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        first_name,
        0x0008,
        tail.len() as u64,
        first_payload.len() as u64,
        Some(checksum::crc32(first_payload)),
    ));
    part02.extend_from_slice(tail);
    part02.extend_from_slice(&build_test_rar_service_header("RR", rr_bytes as u64));
    let rr_at = part02.len();
    part02.extend((0..rr_bytes).map(|index| ((index * 5 + 3) % 256) as u8));
    part02.extend_from_slice(&build_test_rar_file_header(
        second_name,
        0,
        second_payload.len() as u64,
        second_payload.len() as u64,
        Some(checksum::crc32(second_payload)),
    ));
    part02.extend_from_slice(second_payload);
    part02.extend_from_slice(&build_test_rar_end_header(false));

    (
        vec![
            ("silver.horizon.part01.rar".to_string(), part01),
            ("silver.horizon.part02.rar".to_string(), part02),
        ],
        rr_at,
    )
}

#[tokio::test]
async fn retained_envelope_bytes_count_against_the_staged_budget() {
    const RR_BYTES: usize = 48 * 1024;
    const BUDGET: u64 = 16 * 1024;
    let episode = "Silver.Horizon.S01E25.mkv";
    let notes = "Silver.Horizon.S01E25.nfo";
    let episode_payload: Vec<u8> = (0..2000u32).map(|index| (index % 251) as u8).collect();
    let notes_payload: Vec<u8> = (0..60_000u32).map(|index| (index % 197) as u8).collect();
    let (volumes, rr_at) = recovery_record_between_members_set(
        episode,
        &episode_payload,
        notes,
        &notes_payload,
        RR_BYTES,
    );

    // Non-vacuity, part one: the recovery record is wholly inside volume 1's
    // first article, and the second member's data runs well past that article —
    // so the volume is still unconfirmed when the article is routed, and the
    // recovery record is below the last known member extent rather than above
    // it. Those are exactly the conditions under which the bytes are *routed*
    // and then retained.
    let (article_start, article_end) = article_extent(volumes[1].1.len(), 0, 2);
    assert!(
        article_start == 0 && rr_at + RR_BYTES < article_end,
        "the recovery record must sit inside volume 1's first article \
         ({rr_at}+{RR_BYTES} not inside {article_start}..{article_end})"
    );
    assert!(
        volumes[1].1.len() > article_end,
        "the second member's data must reach past that article"
    );

    // Part two: with a budget it cannot breach, the same arrival sequence routes
    // the recovery record into the envelope file. Bytes on disk are routed
    // bytes, not holds — so nothing here is pending, and the breach below can
    // only come from what routing retained.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41053);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the default budget is nowhere near breached by this set, got {shape}"
    );
    let envelope = std::fs::read(working_dir.join("silver.horizon.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "the recovery record was routed into the envelope, so it is not a hold"
    );

    // Part three: the same sequence under a budget smaller than the retained
    // region pages it out (D2). Counting only unrouted holds — as the first
    // shape did — would have found nothing to count at all, so nothing would
    // have breached and nothing would have paged; the retained recovery record
    // is what proves the accounting reaches the term RSS actually pays for.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(BUDGET);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41055);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let paged_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the retained recovery record is {RR_BYTES} bytes of RSS against a {BUDGET}-byte \
         budget and must page rather than demote, got {shape}"
    );
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(
        set.router.scratch_bytes() >= RR_BYTES as u64,
        "the retained recovery record must be what went to scratch, got {} bytes",
        set.router.scratch_bytes()
    );
    assert!(
        set.router.resident_staged_bytes() <= BUDGET,
        "paging must bring RAM back inside the budget, got {}",
        set.router.resident_staged_bytes()
    );
    // The envelope on disk is unaffected: paging moves the router's *retained
    // copy*, never the routed bytes.
    let envelope = std::fs::read(paged_dir.join("silver.horizon.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "paging the retained copy must not disturb what was already written"
    );
}

// ---------------------------------------------------------------------------
// Zero-length stored members (B2)
// ---------------------------------------------------------------------------

/// A two-volume store set whose last volume declares a zero-length member after
/// the split one. An empty stored file is ordinary in a real archive — a
/// placeholder, a `.nfo` that never got written — and RAR states its CRC32 as
/// `0x00000000`, the checksum of no bytes.
fn store_set_with_an_empty_member(
    member_name: &str,
    payload: &[u8],
    empty_name: &str,
) -> Vec<(String, Vec<u8>)> {
    let split = payload.len() / 2;
    let (head, tail) = payload.split_at(split);

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0010,
        head.len() as u64,
        payload.len() as u64,
        Some(checksum::crc32(head)),
    ));
    part01.extend_from_slice(head);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0008,
        tail.len() as u64,
        payload.len() as u64,
        Some(checksum::crc32(payload)),
    ));
    part02.extend_from_slice(tail);
    part02.extend_from_slice(&build_test_rar_file_header(
        empty_name,
        0,
        0,
        0,
        Some(checksum::crc32(&[])),
    ));
    part02.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("silver.horizon.part01.rar".to_string(), part01),
        ("silver.horizon.part02.rar".to_string(), part02),
    ]
}

#[tokio::test]
async fn a_zero_length_member_finalizes_with_its_empty_file_present() {
    // B2: nothing is ever routed for a zero-length member, so the byte-driven
    // whole-member gate can never fire for it. The first shape left it
    // unverified for the life of the job: the set never finalized, never
    // demoted, and kept its D7 suppressions armed over files that would never
    // exist. It verifies trivially instead — CRC32 of no bytes is zero, which is
    // what the header states — and finalization creates the file.
    let episode = "Silver.Horizon.S01E26.mkv";
    let empty = "Silver.Horizon.S01E26.nfo";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 251) as u8).collect();
    let volumes = store_set_with_an_empty_member(episode, &payload, empty);
    let arrivals = in_order_arrivals(volumes.len());
    let names = [episode, empty];

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41055),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41056),
        &names,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a set with an empty member must still route without materializing a volume"
    );
    assert_eq!(
        direct.members[1].1.as_deref(),
        Some(&[][..]),
        "the empty member exists and is empty"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a set with a zero-length member must be byte-identical to the conventional extractor"
    );
}

// ---------------------------------------------------------------------------
// D1's bounded small-member tolerance (plan 135 phase 5 wave 2)
// ---------------------------------------------------------------------------

/// What the extra, ineligible member of a tolerance fixture looks like.
#[derive(Clone, Copy)]
enum ToleranceExtra {
    /// An **unsplit** stored member whose header carries a real BLAKE2sp digest
    /// and no CRC32.
    ///
    /// Unsplit is load-bearing: the classifier only reaches the hash fields once
    /// the chain is complete, so a *split* BLAKE2sp-only member is
    /// `ProvisionallyDirect` — and routes into a partial — until its last header
    /// lands. An unsplit one is `Ineligible` from its single header, so every
    /// byte of it goes to the envelope, which is the shape D1's tolerance
    /// describes and the one the extraction can read back.
    Blake2OnlyStore,
    /// An unsplit compressed member. The data area is not really compressed —
    /// nothing extracts it — so this is only good for what the *classification*
    /// and the budget decide.
    Compressed { declared_unpacked: u64, solid: bool },
    /// A compressed member split across the last two volumes, so its packed
    /// total is a lower bound until the chain closes.
    CompressedSplit,
}

/// A store set of `volume_count` volumes carrying one split stored member plus
/// one extra, ineligible member.
fn store_set_with_extra_member(
    store_name: &str,
    store_payload: &[u8],
    extra_name: &str,
    extra_payload: &[u8],
    volume_count: usize,
    extra: ToleranceExtra,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 2);
    let member_crc = checksum::crc32(store_payload);
    let chunk = store_payload.len().div_ceil(volume_count);
    // The split-compressed shape puts its first half one volume earlier.
    let split_extra = matches!(extra, ToleranceExtra::CompressedSplit);
    let extra_first_volume = if split_extra {
        volume_count - 2
    } else {
        volume_count - 1
    };

    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(store_payload.len());
            let end = ((volume + 1) * chunk).min(store_payload.len());
            let part = &store_payload[start..end];
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
            bytes.extend_from_slice(&build_test_rar_file_header(
                store_name,
                split_flags,
                part.len() as u64,
                store_payload.len() as u64,
                Some(if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                }),
            ));
            bytes.extend_from_slice(part);

            if volume >= extra_first_volume {
                let extra_split = extra_payload.len() / 2;
                let (extra_part, extra_flags): (&[u8], u64) = match (split_extra, is_last) {
                    (true, false) => (&extra_payload[..extra_split], 0x0010),
                    (true, true) => (&extra_payload[extra_split..], 0x0008),
                    (false, _) => (extra_payload, 0),
                };
                let header = match extra {
                    ToleranceExtra::Blake2OnlyStore => build_test_rar_file_header_with_extra(
                        extra_name,
                        extra_flags,
                        extra_part.len() as u64,
                        extra_payload.len() as u64,
                        None,
                        &build_test_rar_blake2_extra(weaver_unrar::crypto::blake2sp_hash(
                            extra_payload,
                        )),
                    ),
                    ToleranceExtra::Compressed {
                        declared_unpacked,
                        solid,
                    } => build_test_rar_compressed_file_header(
                        extra_name,
                        extra_flags,
                        extra_part.len() as u64,
                        declared_unpacked,
                        Some(checksum::crc32(extra_part)),
                        test_rar_compression_info(3, solid),
                    ),
                    ToleranceExtra::CompressedSplit => build_test_rar_compressed_file_header(
                        extra_name,
                        extra_flags,
                        extra_part.len() as u64,
                        extra_payload.len() as u64,
                        Some(checksum::crc32(extra_part)),
                        test_rar_compression_info(3, false),
                    ),
                };
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(extra_part);
            }

            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn a_small_blake2_only_member_rides_the_tolerance_and_both_members_match_the_extractor() {
    let store_name = "Silver.Horizon.S01E15.mkv";
    let extra_name = "Silver.Horizon.S01E15.nfo";
    // The store member has to be at least 100x the extra for the extra to fit
    // under `min(64 MiB, 1% of packed archive bytes)`.
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41041),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41042),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a set riding the tolerance still routes: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[0].1.as_deref(),
        Some(store_payload.as_slice()),
        "the conventional extractor should reproduce the stored member"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the BLAKE2sp-only member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the stored member routes directly and the tolerated one is extracted through \
         the virtual volumes; both must be byte-identical to the conventional extractor"
    );
}

/// The set's final router shape after every article of every volume.
async fn tolerance_shape(job_id: JobId, volumes: &[(String, Vec<u8>)]) -> String {
    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, _) = run_direct_store_routing_only(&temp_dir, job_id, volumes, &arrivals).await;
    shape
}

#[tokio::test]
async fn an_ineligible_member_just_over_the_packed_budget_demotes() {
    let store_name = "Silver.Horizon.S01E16.mkv";
    let extra_name = "Silver.Horizon.S01E16.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();

    // 200 packed bytes against ~30 200 archive bytes is under 1%; 600 is over.
    let under = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &(0..200u32)
            .map(|index| (index % 97) as u8)
            .collect::<Vec<u8>>(),
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let over = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &(0..600u32)
            .map(|index| (index % 97) as u8)
            .collect::<Vec<u8>>(),
        4,
        ToleranceExtra::Blake2OnlyStore,
    );

    let under_shape = tolerance_shape(JobId(41043), &under).await;
    let over_shape = tolerance_shape(JobId(41044), &over).await;

    // The pair is the point: the same fixture on both sides of the ceiling, so
    // the demotion below is the budget and not the shape.
    assert!(
        !under_shape.contains("Demoted"),
        "a member inside the packed budget must ride the tolerance, got {under_shape}"
    );
    assert!(
        over_shape.contains("Demoted(ToleranceBudgetExceeded)"),
        "a member past `min(64 MiB, 1% of packed archive bytes)` must demote, got {over_shape}"
    );
}

#[tokio::test]
async fn a_declared_unpacked_size_over_the_tolerance_ceiling_demotes() {
    let store_name = "Silver.Horizon.S01E17.mkv";
    let extra_name = "Silver.Horizon.S01E17.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();

    // Packed bytes are comfortably inside the budget, so the only ceiling that
    // can fire is the 256 MiB unpacked one — a compressed member declaring an
    // expansion the tolerance will not pay for.
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Compressed {
            declared_unpacked: 300 * 1024 * 1024,
            solid: false,
        },
    );
    let shape = tolerance_shape(JobId(41045), &volumes).await;
    assert!(
        shape.contains("Demoted(ToleranceBudgetExceeded)"),
        "a declared unpacked size over 256 MiB must demote whatever its packed size is, \
         got {shape}"
    );
}

#[tokio::test]
async fn a_provisional_packed_total_that_breaches_at_chain_close_demotes() {
    let store_name = "Silver.Horizon.S01E18.mkv";
    let extra_name = "Silver.Horizon.S01E18.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    // 600 packed bytes total, split in half across the last two volumes: 300 is
    // inside the 1% ceiling, 600 is not.
    let extra_payload: Vec<u8> = (0..600u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::CompressedSplit,
    );

    // Everything up to (and including) the extra's first part: the chain is
    // still open, its packed total is a lower bound inside the budget, and the
    // set keeps routing. Non-vacuity for the demotion below.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut partial_arrivals = in_order_arrivals(volumes.len());
    partial_arrivals.truncate(2 * (volumes.len() - 1));
    let (open_shape, _) =
        run_direct_store_routing_only(&temp_dir, JobId(41046), &volumes, &partial_arrivals).await;
    assert!(
        !open_shape.contains("Demoted"),
        "a provisional packed total inside the budget must keep routing, got {open_shape}"
    );

    let closed_shape = tolerance_shape(JobId(41047), &volumes).await;
    assert!(
        closed_shape.contains("Demoted(ToleranceBudgetExceeded)"),
        "the budget re-check when the chain closes must demote on the true total, \
         got {closed_shape}"
    );
}

#[tokio::test]
async fn a_solid_ineligible_member_demotes_rather_than_riding_the_tolerance() {
    let store_name = "Silver.Horizon.S01E19.mkv";
    let extra_name = "Silver.Horizon.S01E19.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();

    // Identical to a tolerated member in every budget dimension; only the
    // per-member solid flag differs. `extract_member_streaming` can only decode
    // a solid member against the rest of its solid run, so the tolerance must
    // not take it however small it is.
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Compressed {
            declared_unpacked: extra_payload.len() as u64,
            solid: true,
        },
    );
    let shape = tolerance_shape(JobId(41048), &volumes).await;
    assert!(
        shape.contains("Demoted(MemberIneligible(Solid))"),
        "a solid member must demote on its own reason, not on the tolerance budget, \
         got {shape}"
    );
}

// ---------------------------------------------------------------------------
// Phase 5 wave 2 review fixes: what a *later* PAR2 pass sees (B1), what an
// unbindable volume does (B2), and what a mid-download set is protected from
// (H3).
// ---------------------------------------------------------------------------

/// Runs a par2-bearing direct job up to its verification verdict and hands the
/// **live** pipeline back, so a test can keep driving the completion gate.
///
/// Live verification is on, as it is in production: a direct set never enters
/// the archive topology, so `clean_par2_integrity_gate` reads `None` for it and
/// the completion gate would take its repair-first branch — which materializes
/// every live set — rather than letting one finalize. The live short-circuit is
/// what reaches a clean verdict for a par2-bearing direct job.
async fn direct_job_after_verification(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    (pipeline, working_dir)
}

fn no_volume_file(working_dir: &std::path::Path, volumes: &[(String, Vec<u8>)]) -> bool {
    volumes
        .iter()
        .all(|(filename, _)| !working_dir.join(filename).exists())
}

#[tokio::test]
async fn a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass() {
    // B1. Finalization is what makes a direct set's source volumes permanently
    // absent: the partials are renamed to their destinations and the envelopes
    // are deleted. Every *later* pass over the same job — and one conventional
    // member failing extraction after the direct set finalized is enough to
    // cause one, because `par2_validation_needed` is already false so the
    // repair-first branch is skipped — would otherwise read those volumes off a
    // disk they were never on and call the job unrepairable.
    let member_name = "Silver.Horizon.S01E21.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41051);
    let (mut pipeline, working_dir) =
        direct_job_after_verification(&temp_dir, job_id, &volumes, &par2_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Finalized"),
        "the set must have finalized on the job's PAR2 verdict for this test to \
         mean anything, got {sets}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "finalization must not have written a source volume"
    );

    // A conventional member's extraction fails afterwards. That is the whole
    // trigger: `has_crc_failures` re-opens the completion gate on a job whose
    // PAR2 is already marked verified.
    pipeline.failed_extractions.insert(
        job_id,
        ["Amber.Trail.S01E01.mkv".to_string()].into_iter().collect(),
    );
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline.par2_authoritative_verify_calls > verifies_before,
        "non-vacuity: the later authoritative pass must actually have run, \
         calls={verifies_before} -> {}",
        pipeline.par2_authoritative_verify_calls
    );
    let status = job_status_for_assert(&pipeline, job_id);
    if let Some(JobStatus::Failed { error, .. }) = &status {
        panic!("a finalized direct set must not fail a later PAR2 pass, got: {error}");
    }
    assert!(
        no_volume_file(&working_dir, &volumes),
        "the later pass must not have had the repairer reconstruct source volumes \
         the job already finished without; status={status:?}"
    );
    assert!(
        pipeline
            .failed_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty),
        "and the conventional failure must still have been cleared for its retry, \
         exactly as a clean PAR2 verdict does with the gate off"
    );
}

#[tokio::test]
async fn a_direct_volume_with_no_unambiguous_par2_identity_demotes_before_the_pass() {
    // B2. The overlay is keyed by PAR2 file id, so a volume whose identity does
    // not resolve to exactly one description can neither be served virtually nor
    // be blamed for the damage the pass then reports about it. Before this fix
    // the set stayed direct and the repairer was handed a virtual volume.
    let member_name = "Silver.Horizon.S01E22.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41052);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The index first, so the ambiguity is provable before anything acts on it —
    // and so the PAR2 identity pass, which rewrites every file's identity as the
    // set parses, cannot undo it.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    // Volume 0's identity now carries a canonical name that is *another*
    // volume's PAR2 name, so its candidate set spans two descriptions of the
    // same recovery set and `resolve_live_par2_binding` refuses to pick one.
    // This is the shape a rewritten identity produces in the wild; the recovery
    // set itself stays completely honest, which is what lets the job finish
    // conventionally below.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .insert(
            0,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: volumes[0].0.clone(),
                current_filename: volumes[0].0.clone(),
                canonical_filename: Some(volumes[1].0.clone()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        );
    assert!(
        pipeline
            .resolve_live_par2_binding(NzbFileId {
                job_id,
                file_index: 0
            })
            .is_none(),
        "non-vacuity: volume 0's name candidates must match two descriptions, so no \
         single PAR2 identity can be chosen for it"
    );
    assert!(
        pipeline
            .resolve_live_par2_binding(NzbFileId {
                job_id,
                file_index: 1
            })
            .is_some(),
        "and the rest of the set must still bind, so the demotion below is about the \
         one volume rather than about a job with no recovery set"
    );

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(Par2Unbindable)"),
        "a set holding a volume PAR2 cannot name must leave direct mode before the \
         pass, on its own reason, got {sets}"
    );
    // Demotion materialized the volumes from the set's own routed bytes, which
    // is what the pass then reads — nothing repaired a virtual volume.
    for (filename, bytes) in &volumes {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "{filename} must have been materialized byte-exactly by the demotion"
        );
    }

    // The ambiguity was a property of the *identity*, and the fixture's rewritten
    // canonical name is a lie about which file this is — one the conventional
    // path would go on acting on long after it has served its purpose here.
    // Dropped now that the demotion it existed to cause has happened, so what
    // the rest of this test exercises is an ordinary conventional finish over
    // the materialized volumes.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .remove(&0);
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let produced = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| std::fs::read(working_dir.join(member_name)).ok());
    assert_eq!(
        produced.as_deref(),
        Some(payload.as_slice()),
        "and the job must finish conventionally from the materialized volumes; \
         status={:?}",
        job_status_for_assert(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_mid_download_direct_set_is_neither_verified_against_nor_demoted_for_its_holes() {
    // H3. A set that is still receiving articles has holes where the rest of its
    // payload will go, and PAR2 cannot tell a hole from corruption. Both guards
    // are asserted: the completion gate's readiness predicate, and the demotion
    // helper that must not act on such a verdict even if one reaches it.
    let member_name = "Silver.Horizon.S01E23.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41053);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // On, as in production: it is the live short-circuit that reaches a clean
    // verdict for a par2-bearing direct set (a direct set never enters the
    // archive topology, so the clean-integrity gate reads `None` for it).
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The index first, so a recovery set exists to verify against, then every
    // article except the last volume's tail.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (volumes.len() as u32 - 1, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    assert!(
        !pipeline.direct_sets_ready_for_authoritative_par2(job_id),
        "a set whose last volume is still downloading is not ready to be verified"
    );

    // The verdict such a pass would reach: every one of the set's volumes
    // reported missing, which is what a hole looks like from PAR2's side.
    let overlay = pipeline
        .direct_par2_overlay(job_id)
        .expect("the live set's volumes bind and are served virtually");
    let verification = weaver_par2::VerificationResult {
        files: overlay
            .volumes
            .iter()
            .map(|volume| weaver_par2::verify::FileVerification {
                file_id: volume.par2_file_id,
                filename: format!("virtual volume {}", volume.volume_index),
                status: weaver_par2::verify::FileStatus::Missing,
                valid_slices: vec![false; 4],
                missing_slice_count: 4,
            })
            .collect(),
        recovery_blocks_available: 0,
        total_missing_blocks: 4 * overlay.volumes.len() as u32,
        repairable: weaver_par2::verify::Repairability::NotNeeded,
    };
    assert!(
        !pipeline
            .demote_direct_sets_with_par2_damage(job_id, &verification)
            .await,
        "hole-damage on a set that is still downloading must not demote it"
    );
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "the set must still be routing, got {sets}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "and nothing may have materialized or repaired a volume file"
    );

    // The last article lands: the same set is now verifiable, and the gate lets
    // the pass run.
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    submit_volume_article(&mut pipeline, job_id, &volumes, volumes.len() as u32 - 1, 1).await;
    assert!(
        pipeline.direct_sets_ready_for_authoritative_par2(job_id),
        "once every volume has completed the set is ready to be verified"
    );
    let settled = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline.par2_authoritative_verify_calls > verifies_before
            || pipeline.live_par2.metrics().full_verify_skips > 0
            || settled.contains("Finalized"),
        "and verification must have proceeded normally rather than being deferred \
         forever; sets={settled}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "a clean par2-bearing direct job still never writes a source volume"
    );
}

#[tokio::test]
async fn a_tolerated_member_sharing_a_volume_with_a_routed_extent_extracts_before_the_commit() {
    // M4. The tolerated extraction reads the *virtual volumes*: the envelopes
    // overlaid with each direct-routed member's `.direct.partial`. The commit
    // loop renames those partials to their destinations, so running the
    // extraction afterwards pointed the provider's map at paths that no longer
    // existed and turned every stored extent into a hole. It only survived
    // because a RAR header walk seeks over data areas rather than reading them;
    // the moment a read crosses a stored extent the extraction fails, and its
    // failure path is a demotion that can no longer reconstruct — a full
    // redownload.
    //
    // The shape here is the one that makes the dependency real: two volumes the
    // stored member is split across, with the tolerated member's header and data
    // sitting *after* a routed extent inside the last of them, so the walk
    // traverses the region the partial owns to reach it. The ordering itself is
    // held by a `debug_assert!` in `extract_tolerated_members`, which this test
    // exercises with a stored member present.
    let store_name = "Silver.Horizon.S01E24.mkv";
    let extra_name = "Silver.Horizon.S01E24.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 89) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        2,
        ToleranceExtra::Blake2OnlyStore,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41054),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41055),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "the mixed set still routes: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[0].1.as_deref(),
        Some(store_payload.as_slice()),
        "the conventional extractor should reproduce the routed member"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the tolerated member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the tolerated member must be extracted through virtual volumes whose stored \
         extents still resolve, and both members must be byte-identical to the \
         conventional extractor"
    );
}

// ---------------------------------------------------------------------------
// Restart (plan 135, D6) — wave 3
// ---------------------------------------------------------------------------

use crate::pipeline::direct_store::barrier::BarrierDemand;

/// The "before" half of a restart differential.
///
/// Runs a job's first articles with routing on, demands a barrier so the
/// coverage is durable, and drops the pipeline — which is the process going
/// away. The database and the working directory both live under `temp_dir`, so
/// the "after" half opens exactly the state a real restart would find.
async fn direct_store_before_restart(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    articles: usize,
) -> PathBuf {
    direct_store_before_restart_with_password(temp_dir, job_id, volumes, arrivals, articles, None)
        .await
}

/// [`direct_store_before_restart`] with plan 136's extra input. The password is
/// never persisted, so the "after" half has to be handed one of its own.
async fn direct_store_before_restart_with_password(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    articles: usize,
    password: Option<&str>,
) -> PathBuf {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", volumes, articles);
    spec.password = password.map(str::to_owned);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            volumes,
            *file_index,
            *segment_number,
            articles,
        )
        .await;
    }
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    working_dir
}

/// [`queued_segments`] without draining: restart tests need to *read* the queue
/// and then keep feeding the pipeline, and a drained queue makes the completion
/// gate conclude the download is exhausted and fail the job.
fn peek_queued_segments(pipeline: &mut Pipeline, job_id: JobId) -> Vec<(u32, u32)> {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let work = state.download_queue.drain_all();
    let mut out: Vec<(u32, u32)> = work
        .iter()
        .map(|item| {
            (
                item.segment_id.file_id.file_index,
                item.segment_id.segment_number,
            )
        })
        .collect();
    for item in work {
        state.download_queue.push(item);
    }
    out.sort_unstable();
    out
}

/// Pops one article off the queue — standing in for the dispatch that would have
/// fetched it — and feeds its decoded bytes in.
async fn dispatch_and_submit(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
    articles: usize,
) {
    take_queued_segment(
        pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId { job_id, file_index },
            segment_number,
        },
    );
    submit_volume_article_of(
        pipeline,
        job_id,
        volumes,
        file_index,
        segment_number,
        articles,
    )
    .await;
}

/// The "after" half: a fresh pipeline over the same database and working
/// directory, with the job restored through the real restore seam.
///
/// `complete_files` and `file_progress` are deliberately **empty**. D7 keeps
/// both of them empty for a direct set's source volumes — no legacy floor, no
/// completed-file row — so a restore that skips anything at all is skipping it
/// on the strength of the direct checkpoint and nothing else.
async fn direct_store_after_restart(
    temp_dir: &TempDir,
    gate: DirectStoreGate,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    working_dir: &Path,
) -> Pipeline {
    direct_store_after_restart_with_password(
        temp_dir,
        gate,
        job_id,
        volumes,
        articles,
        working_dir,
        None,
    )
    .await
}

/// [`direct_store_after_restart`] with the password the restored job holds.
///
/// `None` is the "operator restarted and the password is gone" case: the set
/// must demote by name rather than wedge, because nothing in the checkpoint can
/// supply one.
#[allow(clippy::too_many_arguments)]
async fn direct_store_after_restart_with_password(
    temp_dir: &TempDir,
    gate: DirectStoreGate,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    working_dir: &Path,
    password: Option<&str>,
) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(false);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", volumes, articles);
    spec.password = password.map(str::to_owned);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.to_path_buf(),
        })
        .await
        .unwrap();
    pipeline
}

/// Reads the member out of wherever the gate left it, the same two candidate
/// places every other differential here checks.
fn member_after_gate(
    complete_dir: &Path,
    working_dir: &Path,
    member_name: &str,
) -> (Option<Vec<u8>>, Option<&'static str>) {
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    match (
        std::fs::read(output_root.join(member_name)).ok(),
        std::fs::read(working_dir.join(member_name)).ok(),
    ) {
        (Some(bytes), _) => (Some(bytes), Some("complete")),
        (None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None) => (None, None),
    }
}

/// The headline restart differential.
///
/// Volume 0 arrives whole, volume 1 arrives half. After the restart the
/// checkpoint's floors must keep every article below them off the download
/// queue, everything above them must come back, and the finished member must be
/// byte-identical to a run that was never interrupted — and to the conventional
/// extractor.
#[tokio::test]
async fn a_mid_download_restart_honours_its_floors_and_completes_byte_identically() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E30.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41060);
    // Volume 0 complete, volume 1 half — so the restore has one file it can skip
    // entirely and one it must skip only part of.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The set came back from its checkpoint rather than from zero.
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    assert!(
        set.has_restart_seeded_coverage(),
        "coverage restored from a checkpoint is seeded and unverified until it is re-read"
    );
    // D7 re-armed: a restored set's source volumes are still direct, so no
    // legacy floor, completed-file row or archive re-probe may be written for
    // them. The restore itself relies on the same thing in the other direction —
    // no completed-file row exists for a direct volume by construction, so the
    // legacy skip plan contributes nothing and the coverage row is the only
    // reason anything is skipped at all.
    assert!(
        (0..volumes.len() as u32)
            .all(|file_index| pipeline.is_direct_source_file(NzbFileId { job_id, file_index })),
        "every restored source volume must still be suppressed as a direct volume"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "a restored direct set must not enter the archive topology"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(1, 0)),
        "volume 1's checkpointed articles must not be refetched, got {queued:?}"
    );
    // Volume 1's *second* article is checkpointed too and still comes back. The
    // floor counts **decoded** source bytes while the spec's `<segment bytes>`
    // is the yEnc-encoded size (~3% larger), so walking the spec against a
    // decoded floor always stops one article short. That is safe — it refetches
    // — and it is bounded to one article per *partially covered* volume, which
    // is one article per volume in flight. A volume the checkpoint calls
    // complete does not pay it at all, which is what the `complete` bit is for
    // and what volume 0 above demonstrates.
    assert!(
        queued.contains(&(1, 1)) && queued.contains(&(1, 2)) && queued.contains(&(1, 3)),
        "volume 1's uncheckpointed articles must come back, got {queued:?}"
    );
    assert!(
        (0..ARTICLES as u32).all(|segment| queued.contains(&(2, segment))),
        "volume 2 never arrived at all and must be refetched whole, got {queued:?}"
    );

    // Non-vacuity: the restart really did save work.
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

    // Feed exactly what the restore asked for, and nothing else.
    for (file_index, segment_number) in queued.clone() {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "the member gate must have re-read the pre-restart ranges before finalizing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted_member, restarted_location) =
        member_after_gate(&complete_dir, &working_dir, member_name);
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let uninterrupted = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41061),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41062),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate must produce the member"
    );
    assert_eq!(
        (
            restarted_member.as_deref(),
            restarted_location,
            &restarted_status
        ),
        (
            uninterrupted.member.as_deref(),
            uninterrupted.member_location,
            &uninterrupted.status
        ),
        "a restarted direct job must finish exactly as an uninterrupted one"
    );
    assert_eq!(
        (restarted_member.as_deref(), restarted_location),
        (conventional.member.as_deref(), conventional.member_location),
        "a restarted direct job must finish exactly as the conventional extractor"
    );
}

/// A byte corrupted on disk while the process was down is caught by the re-read,
/// not committed (D6).
#[tokio::test]
async fn a_byte_corrupted_while_the_process_was_down_fails_the_member_gate() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E31.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 241) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41063);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Flip a byte the checkpoint claims, while "the process is down".
    let partial = working_dir.join(format!("{member_name}.direct.partial"));
    let mut bytes = std::fs::read(&partial).expect("volume 0 routed into the member partial");
    assert!(!bytes.is_empty(), "the partial must hold routed bytes");
    bytes[10] ^= 0xff;
    std::fs::write(&partial, &bytes).unwrap();

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;
    let queued = peek_queued_segments(&mut pipeline, job_id);
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }

    // The **reason**, not merely "something demoted". A bare `Demoted` passes for
    // a set that never got as far as the re-read — a refused row, a rebuild
    // failure, a missing destination — and this test's whole subject is the
    // re-read catching a byte that changed on disk.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)")
            || shape.contains("Demoted(PartChecksumMismatch)"),
        "corruption introduced while the process was down must fail the re-read gate on a \
         checksum, not on some earlier refusal, got {shape}"
    );
    // The demotion deletes the set's partial outputs rather than committing
    // them, and requeues the volumes for the conventional path — which in this
    // harness has no server behind it, so the job stops here rather than
    // finishing. What matters is that the corrupted bytes were never promoted to
    // a destination.
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member, None,
        "corrupt coverage must not be committed to the member's destination"
    );
    assert!(
        !partial.exists(),
        "a demoted set deletes its partials rather than leaving corrupt bytes behind"
    );
}

/// With the gate off the rows are ignored, the job redownloads conventionally,
/// and the direct-store files nothing claims are swept out of the way.
#[tokio::test]
async fn a_restart_with_the_gate_off_redownloads_and_sweeps_the_orphans() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E32.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 233) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41064);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let partial = working_dir.join(format!("{member_name}.direct.partial"));
    let envelope = working_dir.join("silver.horizon.vol00000.envelope");
    assert!(partial.exists() && envelope.exists(), "non-vacuity");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Disabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a disabled gate must redownload the whole job, got {queued:?}"
    );
    assert!(!partial.exists(), "the orphaned partial must be swept");
    assert!(!envelope.exists(), "the orphaned envelope must be swept");
    // Rows are **ignored**, not deleted: a re-enabled binary can still judge
    // them, and it refuses them on the destination probe.
    let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(
        !rows.is_empty(),
        "a disabled gate must not destroy coverage a re-enabled one could judge"
    );
}

/// A row written against a different layout plan is refused, its files are
/// swept, and the row is deleted.
#[tokio::test]
async fn a_digest_mismatch_sweeps_the_sets_files_and_deletes_the_row() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E33.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41065);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Corrupt the row's plan digest by rewriting the blob's tail. Any decode or
    // digest failure lands on the same refusal path.
    {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        let (set_name, blob) = rows.into_iter().next().expect("a committed coverage row");
        let mut corrupted = blob;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        pipeline
            .db
            .save_direct_coverage(job_id, &set_name, &corrupted)
            .unwrap();
    }

    let partial = working_dir.join(format!("{member_name}.direct.partial"));
    assert!(partial.exists(), "non-vacuity");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused row must redownload the whole set, got {queued:?}"
    );
    assert!(
        !partial.exists(),
        "a refused set's partial must be swept before it redownloads"
    );
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "a refused row is deleted"
    );
}

/// Holds scratch from a killed run is swept at restore: it is append-only and
/// meaningless without the in-memory index that named its regions.
#[tokio::test]
async fn restart_sweeps_stale_holds_scratch() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E34.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 227) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41066);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // A killed run's scratch for a set this spec no longer produces: no plan
    // names it, so the prefix rule is the only thing that can find it.
    let stale_scratch = working_dir.join(".weaver-holds.a-set-that-is-gone");
    std::fs::write(&stale_scratch, vec![0u8; 4096]).unwrap();
    let unrelated = working_dir.join("keep-me.txt");
    std::fs::write(&unrelated, b"not direct-store's").unwrap();

    // Collateral the sweep used to take with it. `.envelope` is an extension a
    // user's archive can carry, and the walk descends eight levels into a tree
    // whose shape the archive controls — so a member extracted under that name is
    // a file the sweep must leave completely alone.
    let extracted_member = working_dir.join("chapter.envelope");
    std::fs::write(&extracted_member, b"an extracted member, not an envelope").unwrap();
    let nested = working_dir.join("Season 01");
    std::fs::create_dir_all(&nested).unwrap();
    let nested_member = nested.join("notes.envelope");
    std::fs::write(&nested_member, b"also a member").unwrap();
    // …and a holds-scratch *name* below the top level is not holds scratch
    // either: the real one lives at the working-directory root by construction.
    let nested_lookalike = nested.join(".weaver-holds.not-really");
    std::fs::write(&nested_lookalike, b"still not direct-store's").unwrap();

    let _pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    assert!(
        !stale_scratch.exists(),
        "scratch from a killed run has no index and must be swept"
    );
    assert!(
        unrelated.exists(),
        "the sweep must only touch direct-store's own files"
    );
    for kept in [&extracted_member, &nested_member, &nested_lookalike] {
        assert!(
            kept.exists(),
            "{} is a file the archive named, not one direct-store owns; the sweep must \
             not delete it",
            kept.display()
        );
    }
}

/// A restart inside the PAR2 finalization wait — the common case since wave 2,
/// because a par2-bearing set stays byte-complete-but-uncommitted for the whole
/// PAR2 download and verify.
///
/// Nothing of the set may be refetched: only the PAR2 index is still owed.
#[tokio::test]
async fn a_restart_during_the_par2_wait_refetches_nothing_of_the_set() {
    let member_name = "Silver.Horizon.S01E35.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41070);
    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);

    // Phase one: every volume arrives, the PAR2 index does not. The set is
    // routed, gated and byte-complete, and finalization is waiting.
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        pipeline.live_par2.set_enabled(true);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number)
                .await;
        }
        let set = pipeline
            .direct_store
            .set(job_id, 0)
            .expect("the set must still exist: finalization is waiting for PAR2");
        assert!(
            set.all_volumes_complete() && !set.is_finalized() && !set.is_demoted(),
            "the set must be byte-complete and unfinalized, which is the window this test is about"
        );
        // The last volume's completion already demanded a `PhaseChange` barrier,
        // so the coverage is durable without a shutdown; demanding shutdown here
        // only proves the row is there to read.
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(true);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued
            .iter()
            .all(|(file_index, _)| *file_index == index_file_index),
        "a byte-complete set must refetch nothing; only the PAR2 index is still owed, got {queued:?}"
    );
    assert!(
        !queued.is_empty(),
        "non-vacuity: the PAR2 index really was still outstanding"
    );

    take_queued_segment(
        &mut pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: index_file_index,
            },
            segment_number: 0,
        },
    );
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;

    // The set is byte-perfect — its virtual volumes read back exactly the
    // volumes the conventional gate would have written — and nothing of it was
    // refetched. It must now also *stay* direct.
    //
    // The gate that used to break this is upstream of direct-store:
    // `clean_par2_integrity_gate` was computed from the job's **archive
    // topology**, which a direct set never enters by construction (D7). While
    // the job is live that never showed, because live PAR2 (phase 2) verifies
    // from the decode buffer and short-circuits the authoritative pass — rev 9's
    // "live PAR2 is load-bearing". After a restart there is no decode buffer: no
    // article of the set arrives, live PAR2 has nothing to hash, the gate read
    // `None`, and the completion gate took its repair branch — which materializes
    // every still-routing set and redownloads a set that was already perfect.
    // Direct RAR sets now contribute `StrongDecode` themselves.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a byte-complete direct set must survive a par2-bearing restart rather than \
         being materialized and redownloaded, got {shape}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        (member.as_deref(), location),
        (Some(payload.as_slice()), Some("complete")),
        "the restarted par2-bearing job must finish where the conventional gate does"
    );
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "and must still never materialize a source volume"
    );
}

/// The predicate that keeps the `StrongDecode` contribution honest: the fold is
/// job-wide and the strongest contribution wins, so a restored direct RAR set in
/// a job that also carries a conventional split archive must contribute
/// **nothing** — otherwise the split archive's authoritative PAR2 pass is skipped
/// on the strength of member CRCs that say nothing about it.
///
/// The set is restored rather than live so the *other* two predicates are
/// satisfied and `only_rar_archives` is the one thing deciding the outcome.
#[tokio::test]
async fn a_restored_direct_set_beside_a_split_archive_still_runs_the_authoritative_pass() {
    let member_name = "Silver.Horizon.S01E42.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let split_bytes: Vec<u8> = (0..1500u32).map(|index| (index % 211) as u8).collect();

    // One PAR2 index describing the RAR volumes *and* the split archive's parts,
    // so a single par2 set covers both populations of the job.
    let split_parts: Vec<(String, Vec<u8>)> = (0..2usize)
        .map(|part| {
            let chunk = split_bytes.len().div_ceil(2);
            let start = part * chunk;
            let end = ((part + 1) * chunk).min(split_bytes.len());
            (
                format!("silver.horizon.iso.{:03}", part + 1),
                split_bytes[start..end].to_vec(),
            )
        })
        .collect();
    let mut all_files = volumes.clone();
    all_files.extend(split_parts.iter().cloned());
    let described: Vec<(&str, &[u8])> = all_files
        .iter()
        .map(|(filename, bytes)| (filename.as_str(), bytes.as_slice()))
        .collect();
    let par2_bytes = build_test_par2_index_for_files(&described, PAR2_SLICE_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41082);
    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &all_files, &par2_bytes);

    // Phase one: everything but the PAR2 index arrives, then the process dies
    // inside the finalization wait — the restart the gate change is about.
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        pipeline.live_par2.set_enabled(false);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(all_files.len()) {
            submit_volume_article(
                &mut pipeline,
                job_id,
                &all_files,
                file_index,
                segment_number,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    // The split archive is not direct-store's, so restart owes its articles;
    // feeding them back is what puts it into the archive topology.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    for (file_index, segment_number) in queued {
        if file_index == index_file_index {
            continue;
        }
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &all_files,
            file_index,
            segment_number,
            2,
        )
        .await;
    }

    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| set.was_restored() && !set.is_demoted()),
        "non-vacuity: the RAR set really did come back from its checkpoint, which is \
         what satisfies the gate's other two predicates"
    );
    assert!(
        pipeline.jobs.get(&job_id).is_some_and(|state| state
            .assembly
            .archive_topologies()
            .values()
            .any(|topology| topology.archive_type != crate::jobs::assembly::ArchiveType::Rar)),
        "non-vacuity: the split archive really is in the topology as a non-RAR archive, \
         so `only_rar_archives` is false and is the predicate under test"
    );

    assert!(
        !pipeline.direct_rar_contributes_strong_decode(job_id),
        "a restored direct RAR set must contribute nothing to a job that also carries a \
         conventional split archive: the fold is job-wide and the strongest contribution \
         wins, so contributing here skips the authoritative pass for the split archive — \
         whose integrity the RAR members' CRC32s say nothing about"
    );

    // The other side of the same predicate, through the same harness: the RAR
    // volumes alone — the identical set, restored the identical way — do earn the
    // contribution. Without this the assertion above passes for any reason at
    // all, including the contribution being dead.
    let rar_only_job = JobId(41083);
    let (rar_spec, _) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let rar_working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        pipeline.live_par2.set_enabled(false);
        let working_dir = insert_active_job(&mut pipeline, rar_only_job, rar_spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            submit_volume_article(
                &mut pipeline,
                rar_only_job,
                &volumes,
                file_index,
                segment_number,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };
    let (mut rar_pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    rar_pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    rar_pipeline.live_par2.set_enabled(false);
    rar_pipeline
        .restore_job(RestoreJobRequest {
            job_id: rar_only_job,
            job_hash: [0; 32],
            spec: rar_spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: rar_working_dir,
        })
        .await
        .unwrap();
    // The contribution is only earned once D6's gate re-arm has re-read the
    // restored bytes — that is the third predicate — and the re-arm runs at the
    // download/verify boundary, which for a byte-complete set is here.
    assert!(
        rar_pipeline
            .direct_store
            .set(rar_only_job, 0)
            .is_some_and(|set| set.was_restored() && set.has_restart_seeded_coverage()),
        "non-vacuity: the set came back seeded and unverified, which is the state the \
         third predicate refuses"
    );
    assert!(
        !rar_pipeline.direct_rar_contributes_strong_decode(rar_only_job),
        "a set still carrying unverified restart-seeded bytes has decoded nothing this \
         run and must not claim decode strength"
    );
    rar_pipeline.finalize_ready_direct_sets(rar_only_job).await;
    assert!(
        rar_pipeline.direct_rar_contributes_strong_decode(rar_only_job),
        "non-vacuity: the same restored set in an all-RAR job does earn the contribution \
         once its gates are re-armed, so the refusal above is the split archive's doing \
         and not a dead predicate"
    );
}

/// D2's last failure mode: the scratch file cannot be opened at all.
#[tokio::test]
async fn a_scratch_io_failure_demotes_the_set() {
    let member_name = "Silver.Horizon.S01E36.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41072);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // A directory where the scratch file belongs: every open of it fails, on
    // every platform, without needing permission games.
    std::fs::create_dir(working_dir.join(".weaver-holds.silver.horizon.f0")).unwrap();

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchFailed)"),
        "a scratch that cannot be written must demote with its own reason, got {shape}"
    );
}

/// D6's pause demand, driven through the command seam it is wired at.
#[tokio::test]
async fn pausing_a_job_with_dirty_direct_coverage_demands_a_barrier() {
    let member_name = "Silver.Horizon.S01E37.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41073);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let generation = |pipeline: &Pipeline| -> u64 {
        pipeline
            .db
            .load_direct_coverage(job_id)
            .unwrap()
            .values()
            .next()
            .map(|blob| {
                crate::pipeline::direct_store::snapshot::decode(blob)
                    .unwrap()
                    .generation
            })
            .unwrap_or(0)
    };
    // Neither the byte threshold nor the 5 s timer has fired: the coverage is
    // dirty and uncheckpointed, which is exactly the state a pause must not
    // leave behind.
    let before = generation(&pipeline);

    let (reply, _rx) = tokio::sync::oneshot::channel();
    let command = SchedulerCommand::PauseJob { job_id, reply };
    let scope = Pipeline::pause_barrier_scope(&command)
        .expect("a pause command must be classified as a barrier demand");
    pipeline.demand_direct_store_barriers_for_pause(scope).await;

    let after = generation(&pipeline);
    assert!(
        after > before,
        "pausing a job with dirty direct coverage must advance the row's generation \
         ({before} -> {after})"
    );

    // And a command that is not a pause raises no demand at all.
    let (reply, _rx) = tokio::sync::oneshot::channel();
    assert!(
        Pipeline::pause_barrier_scope(&SchedulerCommand::ResumeJob { job_id, reply }).is_none(),
        "only pause commands demand a barrier"
    );
}

/// Reads the one accepted coverage row of a job back out of the database.
fn coverage_snapshot_of(
    pipeline: &Pipeline,
    job_id: JobId,
) -> crate::pipeline::direct_store::snapshot::CoverageSnapshot {
    let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
    let blob = rows
        .values()
        .next()
        .expect("the set must have checkpointed at least once");
    crate::pipeline::direct_store::snapshot::decode(blob).expect("the row must decode")
}

/// B1: the checkpoint's per-volume `complete` bit means *all bytes durable*, and
/// restart skips every segment of the file on the strength of it.
///
/// A volume can finish downloading while every one of its bytes is still held —
/// here a middle volume arrives whole before the volume whose chain would let
/// the layout place it — and a bit latched at the article-complete seam
/// checkpoints `{floor: header prefix only, complete: true}`. Restart then skips
/// every segment of a volume whose payload does not exist, and the set can
/// neither finalize (its member gate has nothing to compose) nor demote (its
/// reconstruction has nothing to read): a permanent zombie.
#[tokio::test]
async fn a_volume_completing_into_held_bytes_is_not_checkpointed_complete() {
    const ARTICLES: usize = 2;
    const HELD: u32 = 1;
    let member_name = "Silver.Horizon.S01E40.mkv";
    let payload: Vec<u8> = (0..6000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41080);
    // The middle volume arrives whole while the volume that starts the member's
    // chain is entirely absent, so the layout cannot place a byte of it.
    let arrivals: Vec<(u32, u32)> = vec![(HELD, 0), (HELD, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // What the row actually says about that volume, read straight out of the
    // database the restart is about to consult.
    {
        let (probe, _, _) = new_direct_pipeline(&temp_dir).await;
        let snapshot = coverage_snapshot_of(&probe, job_id);
        let entry = snapshot
            .floors
            .iter()
            .find(|entry| entry.file_index == HELD)
            .expect("the completed volume must appear in the checkpoint");
        // Only the volume's own header prefix could be classified — its payload
        // belongs to a member chain the layout cannot place without the volume
        // before it — so the floor stops far short of the volume.
        assert!(
            entry.floor > 0 && entry.floor < volumes[HELD as usize].1.len() as u64,
            "non-vacuity: this volume's payload really is all held, so its floor covers \
             only the header prefix (floor {} of {} bytes)",
            entry.floor,
            volumes[HELD as usize].1.len()
        );
        assert!(
            !entry.complete,
            "a volume whose floor covers none of its payload must not be checkpointed \
             complete: restart would skip every segment of a volume whose bytes do not \
             exist ({entry:?})"
        );
    }

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        (0..ARTICLES as u32).all(|segment| queued.contains(&(HELD, segment))),
        "every segment of a volume whose bytes were held must come back, got {queued:?}"
    );
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "only a header prefix was durable, so the whole job is owed, got {queued:?}"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the job must finish rather than wedge on a volume it was told it already had"
    );
}

/// B2's other half: a restored volume whose header walk the previous run never
/// finished cannot be confirmed by a parse — its pre-restart bytes are on disk
/// rather than in the staged image — and the format cannot vouch for its tail
/// either, because the volume *closes* the member chain and a second member's
/// header could sit past the first's data area.
///
/// It must therefore leave direct mode by the front door, with its own reason,
/// rather than hold its end-of-archive record for the life of the set and let a
/// byte-perfect set read short to PAR2.
#[tokio::test]
async fn a_restored_last_volume_that_cannot_reconfirm_demotes_by_name() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E41.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41081);
    // Restart lands mid-download of the **last** volume: its end-of-archive
    // record has not arrived, so the cached facts stop short of it.
    let arrivals: Vec<(u32, u32)> = vec![
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 0),
        (2, 1),
    ];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted()),
        "non-vacuity: the set came back from its checkpoint still routing"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.iter().any(|(file_index, _)| *file_index == 2),
        "non-vacuity: the last volume really was still owed articles, got {queued:?}"
    );
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(UnconfirmedRestoredVolume)"),
        "a restored volume that can never be confirmed must demote under its own \
         reason rather than hold its tail, got {shape}"
    );
    // Never wedged. The demotion is a *transition*, not a dead end: the set's
    // routed output is gone and the conventional path owns the volumes now,
    // either as materialized files or as work back on the queue. What must not
    // happen — and what holding the tail produced — is a set that stays direct,
    // finalizes nothing and refetches nothing.
    let materialized = volumes
        .iter()
        .filter(|(filename, _)| working_dir.join(filename).exists())
        .count();
    let requeued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        materialized > 0 || !requeued.is_empty(),
        "a demoted set must hand its volumes to the conventional path, either \
         materialized or refetched (materialized {materialized}, requeued {requeued:?})"
    );
    for (filename, bytes) in &volumes {
        let Some(on_disk) = std::fs::read(working_dir.join(filename)).ok() else {
            continue;
        };
        assert_eq!(
            on_disk.as_slice(),
            bytes.as_slice(),
            "{filename} was materialized, so it must be byte-exact"
        );
    }
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member, None,
        "an unconfirmed volume's set must not commit a member it could not prove"
    );
}

// ---------------------------------------------------------------------------
// Phase 6 — repair while still direct (plan 135, D8)
// ---------------------------------------------------------------------------

/// A PAR2 index over the set's decoded volume bytes that also carries
/// **recovery blocks**, so the damage it describes can actually be repaired.
///
/// `build_test_par2_index_for_files` stops at descriptions and slice checksums,
/// which is why every damaged-set test before phase 6 could only assert a
/// verdict: with no recovery stream neither gate can repair, so "repairs while
/// direct" had nothing to compare against. The blocks are computed over the
/// global input-slice ordering PAR2 defines — files in main-packet order, slices
/// in order within each file, each padded to `slice_size` — which is the same
/// ordering `plan_repair` reconstructs from the parsed set.
fn build_test_par2_with_recovery(
    files: &[(&str, &[u8])],
    slice_size: u64,
    recovery_block_count: usize,
) -> Vec<u8> {
    let mut stream = build_test_par2_index_for_files(files, slice_size);
    if recovery_block_count == 0 {
        return stream;
    }
    // Recomputed rather than parsed back: the recovery-set id is the MD5 of the
    // main packet body, and every packet's own hash covers it, so the two
    // builders have to agree on the derivation or nothing merges.
    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for (filename, data) in files {
        let hash_16k = checksum::md5(&data[..data.len().min(16 * 1024)]);
        let mut file_id_input = Vec::new();
        file_id_input.extend_from_slice(&hash_16k);
        file_id_input.extend_from_slice(&(data.len() as u64).to_le_bytes());
        file_id_input.extend_from_slice(filename.as_bytes());
        main_body.extend_from_slice(&checksum::md5(&file_id_input));
    }
    let recovery_set_id = checksum::md5(&main_body);

    let slice_size_bytes = slice_size as usize;
    let word_count = slice_size_bytes / 2;
    // Every input slice of every file, padded, concatenated in PAR2's global
    // ordering.
    let mut padded: Vec<u8> = Vec::new();
    for (_, data) in files {
        let slices = (data.len() as u64).div_ceil(slice_size) as usize;
        let mut block = data.to_vec();
        block.resize(slices * slice_size_bytes, 0);
        padded.extend_from_slice(&block);
    }
    let slice_count = padded.len() / slice_size_bytes;
    let constants = weaver_par2::input_slice_constants(slice_count);

    for exponent in 0..recovery_block_count as u32 {
        let mut recovery = vec![0u8; slice_size_bytes];
        for (input_index, &constant) in constants.iter().enumerate() {
            let factor = weaver_par2::gf_pow(constant, exponent);
            for word_index in 0..word_count {
                let at = input_index * slice_size_bytes + word_index * 2;
                let input_word = u16::from_le_bytes([padded[at], padded[at + 1]]);
                let contribution = weaver_par2::gf_mul(input_word, factor);
                let current =
                    u16::from_le_bytes([recovery[word_index * 2], recovery[word_index * 2 + 1]]);
                let updated = weaver_par2::gf_add(current, contribution).to_le_bytes();
                recovery[word_index * 2] = updated[0];
                recovery[word_index * 2 + 1] = updated[1];
            }
        }
        let mut body = Vec::with_capacity(4 + slice_size_bytes);
        body.extend_from_slice(&exponent.to_le_bytes());
        body.extend_from_slice(&recovery);
        stream.extend_from_slice(&build_test_par2_packet(
            weaver_par2::packet::header::TYPE_RECOVERY,
            &body,
            recovery_set_id,
        ));
    }
    stream
}

fn repairable_par2_index(volumes: &[(String, Vec<u8>)], recovery_blocks: usize) -> Vec<u8> {
    let described: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(filename, bytes)| (filename.as_str(), bytes.as_slice()))
        .collect();
    build_test_par2_with_recovery(&described, PAR2_SLICE_BYTES, recovery_blocks)
}

/// What one repairable-damage gate produced.
#[derive(Debug)]
struct RepairGateOutcome {
    status: Option<JobStatus>,
    member: Option<Vec<u8>>,
    /// Whether any source volume file existed at any point. For the direct gate
    /// this must stay false: repair-while-direct materializes only the *damaged*
    /// volumes, and it does so under a scratch name, never the volume's own.
    volume_file_seen: bool,
    /// Repair scratch left behind. Always zero — the temporaries are deleted
    /// whether the repair succeeded or fell back.
    repair_scratch_left: usize,
    /// The set's `Debug` shape immediately after verification concluded.
    sets: String,
    /// Source volumes a repair materialized. The scratch is deleted as soon as
    /// its spans are routed, so nothing on disk can distinguish "materialized
    /// one volume" from "materialized every volume and tidied up".
    materialized: usize,
    /// Sets that committed their members from their own partials. Sticky,
    /// because a set can finalize, complete its job and be pruned inside one
    /// completion check — so `sets` below may never show the state.
    finalized: usize,
}

/// Keeps the last non-empty reading of a job's direct sets, and never lets a
/// later one hide a `Finalized` it already saw.
///
/// The runtime is pruned the moment the job finishes, and finalization is a
/// different completion check from the one that repairs — so a single snapshot
/// can only ever show one of the two, and which one it shows depends on how many
/// steps the harness happened to take.
fn sample_direct_sets(pipeline: &Pipeline, job_id: JobId, sets: &mut String) {
    let current = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    if current == "[]" {
        return;
    }
    if sets.contains("Finalized") && !current.contains("Finalized") {
        return;
    }
    *sets = current;
}

/// Every path a direct set could have materialized a volume at, damaged or not.
fn direct_scratch_left(working_dir: &Path) -> usize {
    let mut left = 0usize;
    let Ok(entries) = std::fs::read_dir(working_dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".repair") {
            left += 1;
        }
    }
    left
}

/// Where the PAR2 index sits **in the NZB**, which decides whether a set's
/// volume indices and its job file indices happen to be the same numbers.
///
/// Appended last is the usual posting order, and it is also the one that hides
/// bugs: the set's volumes are then files `0..n-1`, so volume index and file
/// index coincide and a seam that confuses the two still works. Leading, they
/// never agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexPosition {
    First,
    Last,
}

impl IndexPosition {
    /// The NZB file index of the set's volume `ordinal`.
    fn volume_file_index(self, ordinal: u32) -> u32 {
        match self {
            Self::First => ordinal + 1,
            Self::Last => ordinal,
        }
    }
}

/// [`par2_bearing_job_spec`] with the index placed at either end of the NZB.
fn par2_bearing_job_spec_positioned(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
) -> (JobSpec, u32) {
    let (mut spec, file_index) = par2_bearing_job_spec(name, volumes, par2_bytes);
    match position {
        IndexPosition::Last => (spec, file_index),
        IndexPosition::First => {
            let index = spec.files.remove(file_index as usize);
            spec.files.insert(0, index);
            (spec, 0)
        }
    }
}

async fn run_repairable_par2_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
) -> RepairGateOutcome {
    run_repairable_par2_gate_at(
        gate,
        job_id,
        member_name,
        volumes,
        par2_bytes,
        IndexPosition::Last,
    )
    .await
}

async fn run_repairable_par2_gate_at(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
) -> RepairGateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) =
        par2_bearing_job_spec_positioned("Silver Horizon", volumes, par2_bytes, position);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (ordinal, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article_indexed(
            &mut pipeline,
            job_id,
            volumes,
            ordinal,
            position.volume_file_index(ordinal),
            segment_number,
        )
        .await;
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    // Tracked rather than snapshotted once: verification runs when the index
    // completes, but finalization is a *later* completion check, and a job that
    // finishes has its direct-store runtime pruned — so the last non-empty
    // reading is the only one that can show both.
    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    volume_file_seen |= volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());

    // The harness delivers articles without dequeuing them, so the download
    // pipeline never looks exhausted; draining is what lets the repair gate
    // reach its verdict, and both gates get it.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    // Deliberately not `drive_extractions_to_terminal`: that helper blocks on
    // the extraction channel and panics on a job that is legitimately not going
    // to extract, which the unrepairable variant of this gate is. This one polls
    // instead and lets the caller assert whatever the job actually reached.
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        settle_inflight_moves(&mut pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    RepairGateOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        member: std::fs::read(output_root.join(member_name))
            .ok()
            .or_else(|| std::fs::read(working_dir.join(member_name)).ok()),
        volume_file_seen,
        repair_scratch_left: direct_scratch_left(&working_dir),
        sets,
        materialized: pipeline.direct_store.repair_materialized_volumes,
        finalized: pipeline.direct_store.finalized_sets,
    }
}

#[tokio::test]
async fn par2_damage_in_the_envelope_repairs_while_the_set_stays_direct() {
    // D8's first transition, end to end. The damaged byte is in a recovery
    // record's data area: outside every member's packed range, so neither the
    // per-part packed CRC32 nor the whole-member CRC32 covers it, and inside a
    // service block's data rather than a header, so the walk still parses and
    // the volume still confirms. PAR2 is the only layer that can see it — which
    // is exactly the set-up wave 2 could only demote on.
    let member_name = "Silver.Horizon.S01E21.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    // The PAR2 set describes the *clean* volumes and carries enough recovery to
    // rebuild the damaged slice; the job downloads a damaged volume.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41061),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;
    let direct = run_repairable_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41062),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and extract the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        direct.member.as_deref(),
        conventional.member.as_deref(),
        "a direct set must repair in place and produce the same bytes the \
         gate-off gate produces after its repair; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "the set must stay direct through the repair — that is the whole \
         transition, and demoting instead costs a materialization of every \
         volume; got {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "and it must finalize once the re-verify clears it — committing its own \
         partials, which is the only way a member reaches its destination \
         without a volume file ever existing; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume may materialize under its own name: that path belongs to \
         demotion, and a half-repaired file sitting there would be read as a \
         downloaded volume by every conventional path"
    );
    assert_eq!(
        direct.materialized, 1,
        "exactly the one damaged volume materializes. The other two are read as \
         repair *sources* through the hybrid provider, which is what makes the \
         expansion set empty by construction rather than merely small"
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "the repair scratch is deleted once its spans are routed, so the set \
         returns to fully virtual"
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn a_repair_reads_its_sources_by_file_index_when_the_par2_index_leads_the_nzb() {
    // The index-space regression, and the reason every other phase 6 fixture is
    // blind to it. A repair materializes its damaged volumes through the hybrid
    // provider, and that provider is keyed by **job file index** so one instance
    // can answer for every set of a job. The reconstruction plan was built with
    // the set's own **volume index**. The two are the same number exactly when a
    // set's volumes are NZB files `0..n-1` — true whenever the PAR2 is appended
    // last, which is every fixture and most real NZBs, and false the moment a
    // `.par2` or `.nfo` leads or a job carries two sets.
    //
    // When they differ the sweep reads a *different volume's* bytes: volume 1
    // asks for file index 1 and is handed volume 0, fails its composed CRC32,
    // and the whole set demotes with `materialization_failed` as the only signal
    // that anything went wrong. Volume 0 asks for file index 0 — the PAR2 index
    // — and finds nothing at all.
    //
    // Same fixture and same damage as
    // `par2_damage_in_the_envelope_repairs_while_the_set_stays_direct`, with the
    // index moved to NZB position 0 and nothing else changed.
    let member_name = "Silver.Horizon.S01E26.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let direct = run_repairable_par2_gate_at(
        DirectStoreGate::Enabled,
        JobId(41065),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::First,
    )
    .await;

    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the repair must produce the member byte for byte, whatever number the \
         NZB happens to give the set's volumes; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and the set must stay direct: reading the wrong volume's bytes fails a \
         composed CRC32 and demotes the whole set, which is the failure this \
         asserts against; got {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "still exactly the one damaged volume; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume may materialize under its own name"
    );
    assert_eq!(direct.repair_scratch_left, 0);
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn an_unrepairable_direct_set_still_demotes_whole() {
    // The fallback, unchanged: with no recovery blocks the damage cannot be
    // repaired, so phase 6 refuses before it materializes anything and wave 2's
    // whole-set demotion answers instead. Same fixture, same damage, one
    // difference — the recovery stream.
    let member_name = "Silver.Horizon.S01E22.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&clean, 0);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let direct = run_repairable_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41063),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert!(
        direct.sets.contains("Demoted(Par2Damaged)"),
        "an unrepairable direct set must demote whole, exactly as it did before \
         phase 6, got {}",
        direct.sets
    );
    assert!(
        direct.volume_file_seen,
        "and the demotion must materialize its volumes for the conventional path"
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "a refused repair leaves no scratch behind"
    );
    assert!(
        !matches!(direct.status, Some(JobStatus::Complete)),
        "an unrepairable job must not complete, got {:?}",
        direct.status
    );
}

/// A par2-bearing direct job driven to the point where its one set is live and
/// carries repairable PAR2 damage, with the live pipeline handed back so a test
/// can drive phase 6's seam itself and watch what it refuses.
async fn live_damaged_direct_job(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    holds_budget: Option<u64>,
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(true);
    if let Some(bytes) = holds_budget {
        pipeline.direct_store.set_holds_budget(bytes);
    }

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    // The harness delivers articles without dequeuing them, so without this the
    // download pipeline never looks exhausted and H3's settle guard defers every
    // verdict.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    (pipeline, working_dir)
}

/// The envelope-damage fixture every phase 6 repair test is built on: three
/// volumes carrying one stored member and a recovery record, with the record's
/// data area damaged in the middle volume.
fn repairable_envelope_damage(
    member_name: &str,
    payload: &[u8],
) -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean;
    damage_recovery_record(&mut volumes, 1, rr_bytes);
    (volumes, par2_bytes)
}

#[tokio::test]
async fn a_rewrite_over_the_holds_budget_demotes_before_it_materializes_anything() {
    // F3. Every repaired byte re-enters the router as a hold, so a rewrite is
    // charged against the same RAM ceiling ordinary staging is — and the first
    // shape never checked: it read every rewrite span of every damaged volume
    // whole, then let the router copy them into staging, so a missing-volume
    // repair of a large set peaked at about twice the repaired bytes with
    // nothing bounding either term.
    //
    // The A/B is the budget and nothing else. Phase 7 revisits the bound itself
    // — routing a repaired volume in budget-sized instalments lifts it — but
    // until then an over-budget rewrite demotes, and it must do so before the
    // checkpoint delete, so finding out costs the set nothing.
    let member_name = "Silver.Horizon.S01E28.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let inside_dir = tempfile::tempdir().unwrap();
    let (inside, _) =
        live_damaged_direct_job(&inside_dir, JobId(41101), &volumes, &par2_bytes, None).await;
    let inside_sets = format!("{:?}", inside.direct_store.sets_for(JobId(41101)));
    assert_eq!(
        inside.direct_store.repair_materialized_volumes, 1,
        "non-vacuity: inside the default budget this fixture repairs while \
         direct, so the only thing the run below changes is the ceiling; \
         sets = {inside_sets}"
    );
    assert!(
        !inside_sets.contains("Demoted"),
        "and it stays direct doing it; got {inside_sets}"
    );

    let over_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41102);
    // One byte: under every rewrite, and small enough to be unambiguous. Routing
    // itself survives it — holds page out to scratch rather than demoting.
    let (over, working_dir) =
        live_damaged_direct_job(&over_dir, job_id, &volumes, &par2_bytes, Some(1)).await;

    let sets = format!("{:?}", over.direct_store.sets_for(job_id));
    assert_eq!(
        over.direct_store.repair_materialized_volumes, 0,
        "an over-budget repair must refuse before the materialization, not after \
         reading the bytes it exists to avoid reading; sets = {sets}"
    );
    assert_eq!(
        over.direct_store.repair_attempts, 0,
        "and before the checkpoint delete, so it does not spend the set's one \
         repair attempt on a refusal that cost it nothing; sets = {sets}"
    );
    assert!(
        sets.contains("Demoted"),
        "the set then takes the whole-set demotion, which is always correct; \
         got {sets}"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no repair scratch may exist: none was ever opened"
    );
}

#[tokio::test]
async fn a_second_damage_verdict_after_a_repair_demotes_instead_of_repairing_again() {
    // F8. Nothing else terminates the loop. A set that is damaged again after a
    // completed repair produces the same verdict on every completion check, and
    // without a bound each one materializes, repairs, re-routes and re-verifies
    // — forever. One attempt, then the whole-set demotion.
    let member_name = "Silver.Horizon.S01E29.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41103);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_materialized_volumes
        ),
        (1, 1),
        "non-vacuity: the set must have had its one real repair before the \
         second verdict can be a repeat; sets = {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized() && set.repair_attempted()),
        "and it must still be live with its latch burned; got {sets}"
    );

    // Fresh damage under the repaired set, in the member partial the virtual
    // volume reads its member bytes back out of. This is the shape the bound
    // exists for: a repair that did not leave the set verifiable, however it got
    // there.
    let partial = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".direct.partial"))
        })
        .expect("the live set still holds its member partial");
    let mut bytes = std::fs::read(&partial).unwrap();
    bytes[10] ^= 0xFF;
    std::fs::write(&partial, &bytes).unwrap();

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        resolution,
        crate::pipeline::direct_store::wiring::DirectPar2Resolution::Unresolved,
        "a set that has had its attempt must fall through to the demotion rather \
         than report a repair; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "and no second attempt may be made at all — that is the loop, one lap of \
         it. Counted rather than inferred: an attempt that refuses downstream \
         leaves the same traces as one that was never made; sets = {sets}"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no scratch, for the same reason"
    );
}

/// Renames a generated set's volume files onto a different archive base name,
/// so one job can carry two direct sets.
///
/// Safe by construction: a RAR5 volume's own bytes carry its *number* in the
/// main header and nothing about the filename, which is what
/// `archive_base_name` groups on.
fn renamed_set(base: &str, volumes: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    volumes
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| (format!("{base}.part{:02}.rar", index + 1), bytes))
        .collect()
}

#[tokio::test]
async fn a_finalized_set_does_not_stop_its_live_neighbour_repairing_while_direct() {
    // F2. The quiet pass phase 6 runs in front of the repairer used to be a bare
    // `verify_all`, without the two damage-attribution adjustments the
    // authoritative pass applies to its own verdict. A **finalized** direct set
    // has no source volumes on disk and never will — its partials are at their
    // destinations and its envelopes are deleted — so every one of them reads
    // `Missing` in that pass. `damaged_files_by_set` then finds no live owner
    // for them and refuses the whole attempt with `DamageOutsideDirectSets`, so
    // the set that *is* live and *is* repairable demotes instead, for damage
    // that belongs to files the job legitimately finished without.
    let finalized_member = "Silver.Horizon.S01E27.mkv";
    let live_member = "Amber.Trail.S01E01.mkv";
    let finalized_payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 227) as u8).collect();
    let finalized_set = single_member_store_set(finalized_member, &finalized_payload, 2);
    let live_set = renamed_set(
        "amber.trail",
        recovery_record_store_set(live_member, &live_payload, 3, 256),
    );
    let volumes: Vec<(String, Vec<u8>)> = finalized_set
        .iter()
        .chain(live_set.iter())
        .cloned()
        .collect();
    let par2_bytes = repairable_par2_index(&volumes, 16);
    // The live set's middle volume loses its second half — member payload, the
    // recovery record and the end-of-archive record together, which is what
    // keeps that set's member gate open so it does not finalize alongside its
    // neighbour.
    let lost = (finalized_set.len() as u32 + 1, 1u32);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41091);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == lost {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    // The lost article is never coming, so the pass may treat holes as damage.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }

    // Reaching the state directly, because it is a *state*, not a sequence: a
    // job whose PAR2 already read clean once released its ready sets, and one of
    // them being ready while the other is not is the whole shape. Later passes
    // happen for reasons that have nothing to do with the direct sets — a
    // conventional member failing extraction is enough.
    pipeline.par2_verified.insert(job_id);
    pipeline.finalize_ready_direct_sets(job_id).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| set.is_finalized())
            .count(),
        1,
        "non-vacuity: exactly one set must have finalized, or there is no \
         finalized neighbour to be confused by; got {sets}"
    );
    assert_eq!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| !set.is_finalized() && !set.is_demoted())
            .count(),
        1,
        "and exactly one must still be live and damaged; got {sets}"
    );

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    // The observable is **which set the damage was attributed to**, read off the
    // repair once-latch: it is burned inside `repair_one_direct_set` and nowhere
    // else, so it is set exactly when the pass decided this set owned the damage
    // and went to work on it. Before the fix the whole attempt was refused up
    // front with `DamageOutsideDirectSets` — the finalized neighbour's absent
    // volumes counted as damage nobody owns — and no set was ever reached.
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_finalized() && set.repair_attempted()),
        "the live set's damage is its own, so the pass must attribute it there \
         and attempt the repair; a set that was never reached is the finalized \
         neighbour being blamed for it. sets = {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_finalized() || !set.repair_attempted()),
        "and the finalized set is never the one repaired: its volumes are gone \
         on purpose. sets = {sets}"
    );
    assert!(
        live_set
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "nothing may materialize under a live volume's own name while the \
         attempt is in progress"
    );
}

/// [`run_repairable_par2_gate`] with an article that never arrives.
///
/// A **lost article** is the only shape member-payload damage can reach PAR2 in.
/// Corrupted member bytes are caught far earlier and far more cheaply by D4's
/// own gates — the per-part packed CRC32 at part completion, the whole-member
/// CRC32 at member completion — which demote the set during the download, before
/// a PAR2 index has even been parsed. What those gates cannot do is *manufacture*
/// bytes that never came, so a hole in a member's packed range survives to the
/// PAR2 pass, and repairing it is exactly what phase 6 is for.
async fn run_lost_article_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    lost: (u32, u32),
) -> RepairGateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.live_par2.set_enabled(true);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == lost {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    // The lost article is never coming, and the harness has no server to say so
    // — draining the queues is what makes the download pipeline look exhausted,
    // which is the condition every PAR2 gate waits for before treating a hole as
    // damage rather than as work in flight.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        settle_inflight_moves(&mut pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        volume_file_seen |= volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    RepairGateOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        member: std::fs::read(output_root.join(member_name))
            .ok()
            .or_else(|| std::fs::read(working_dir.join(member_name)).ok()),
        volume_file_seen,
        repair_scratch_left: direct_scratch_left(&working_dir),
        sets,
        materialized: pipeline.direct_store.repair_materialized_volumes,
        finalized: pipeline.direct_store.finalized_sets,
    }
}

#[tokio::test]
async fn a_lost_article_inside_a_member_repairs_and_reconfirms_the_volume() {
    // Two things at once, because one article carries both.
    //
    // The lost article is the **second half** of the middle volume: member
    // payload, the recovery record, and the end-of-archive header. So the repair
    // has to route bytes back into a `.direct.partial` at a range the member's
    // coverage map never held — re-arming a whole-member gate that could not
    // previously fire — *and* into the envelope, where the restored end record
    // is what lets the header walk finish and confirm a volume that had no proof
    // no further header could appear.
    let member_name = "Silver.Horizon.S01E23.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 223) as u8).collect();
    let rr_bytes = 256;
    let volumes = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41071),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
    )
    .await;
    let direct = run_lost_article_gate(
        DirectStoreGate::Enabled,
        JobId(41072),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the lost article and extract the \
         member; status={:?}",
        conventional.status
    );
    assert_eq!(
        direct.member.as_deref(),
        conventional.member.as_deref(),
        "a direct set must repair a hole in a member's packed range in place and \
         produce the gate-off bytes; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and it must not demote to do it, got {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "a volume whose end record arrived only in the repaired bytes must still \
         confirm, or the set could never finalize; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume file, repaired or otherwise"
    );
    assert_eq!(
        direct.materialized, 1,
        "only the volume that lost an article materializes"
    );
    assert_eq!(direct.repair_scratch_left, 0);
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

/// [`par2_bearing_job_spec`] with a chosen article count per volume, so a
/// fixture can lose a *middle* article and leave an interior hole rather than a
/// truncated tail.
fn par2_bearing_job_spec_with_articles(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    articles: usize,
) -> (JobSpec, u32) {
    let mut spec = direct_store_job_spec_with_articles(name, volumes, articles);
    let index_filename = "silver.horizon.par2".to_string();
    let file_index = spec.files.len() as u32;
    spec.total_bytes += u64::from(yenc_declared_bytes(par2_bytes.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&index_filename),
        filename: index_filename,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(par2_bytes.len() as u32),
            message_id: "direct-par2-index@example.com".to_string(),
        }],
    });
    (spec, file_index)
}

#[tokio::test]
async fn an_interior_hole_sizes_the_repair_by_the_slices_it_actually_touches() {
    // The wave-2 review note, priced. The middle article of the middle volume is
    // lost, so the volume has an interior hole with healthy bytes on both sides.
    // Before phase 6 the verifier's sequential sweep stopped at that hole and
    // called every slice after it damaged; a repair sized from that count spends
    // a recovery block per slice, and the ones past the hole are blocks spent
    // rebuilding bytes that were never broken. Enough of them and a repairable
    // set reads as unrepairable.
    const ARTICLES: usize = 3;
    let member_name = "Silver.Horizon.S01E24.mkv";
    let payload: Vec<u8> = (0..6000u32).map(|index| (index % 229) as u8).collect();
    let volumes = recovery_record_store_set(member_name, &payload, 3, 256);
    let par2_bytes = repairable_par2_index(&volumes, 64);

    let volume_len = volumes[1].1.len();
    let (hole_start, hole_end) = article_extent(volume_len, 1, ARTICLES);
    // Slices the hole overlaps, and slices from the hole to the volume's end —
    // the honest count and the inflated one. The fixture is only interesting
    // while the two differ.
    let slice = PAR2_SLICE_BYTES as usize;
    let touched = hole_end.div_ceil(slice) - hole_start / slice;
    let to_the_end = volume_len.div_ceil(slice) - hole_start / slice;
    assert!(
        touched < to_the_end,
        "the fixture must have healthy slices after the hole, or the accounting \
         fix is untestable: touched={touched} to_the_end={to_the_end}"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(true);
    let job_id = JobId(41081);
    let (spec, index_file_index) =
        par2_bearing_job_spec_with_articles("Silver Horizon", &volumes, &par2_bytes, ARTICLES);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..ARTICLES as u32 {
            if (file_index, segment_number) == (1, 1) {
                continue;
            }
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                file_index,
                segment_number,
                ARTICLES,
            )
            .await;
        }
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "the interior hole must repair and the member must come out whole; \
         sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_recovery_blocks_used, touched,
        "the repair must spend one recovery block per slice the hole actually \
         touches ({touched}), not one per slice from the hole to the end of the \
         volume ({to_the_end}); sets = {sets}"
    );
    assert_eq!(pipeline.direct_store.repair_materialized_volumes, 1);
    assert_eq!(direct_scratch_left(&working_dir), 0);
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and no volume file may exist at the end of it"
    );
}

// ---------------------------------------------------------------------------
// Phase 7: the config surface, and D3's sparse marking
// ---------------------------------------------------------------------------

/// Every other test here reaches for `set_gate`. This one comes through
/// configuration, which is what phase 7 made the operator surface: the
/// `[direct_store]` table turns routing on, and turning it **off** at startup
/// makes a restart ignore and sweep the mid-flight direct state and redownload
/// the job conventionally (plan 135, Risks — kill switch).
#[tokio::test]
async fn the_config_gate_routes_and_a_config_off_restart_sweeps_and_redownloads() {
    use crate::settings::DirectStoreOverrides;

    // `DirectStoreSettings::resolve` reads the real environment, and the env
    // override deliberately beats config. A developer with the variable
    // exported would be testing the override, not the config.
    if crate::pipeline::direct_store::env_override().is_some() {
        return;
    }

    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E52.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41090);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];

    // Phase 1: config on. The job routes, so partials, envelopes and a coverage
    // row exist — the non-vacuity the sweep assertions below depend on.
    let working_dir = {
        let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
            &temp_dir,
            DirectStoreOverrides {
                enabled: Some(true),
                holds_scratch_ceiling_bytes: None,
            },
        )
        .await;
        pipeline.live_par2.set_enabled(false);
        let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in &arrivals {
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                *file_index,
                *segment_number,
                ARTICLES,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let partial = working_dir.join(format!("{member_name}.direct.partial"));
    let envelope = working_dir.join("silver.horizon.vol00000.envelope");
    assert!(
        partial.exists() && envelope.exists(),
        "the config table must be able to turn routing on at all"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and a config-gated direct job writes no source volume either"
    );

    // Phase 2: config off. Same working directory, same spec, a fresh pipeline.
    let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
        &temp_dir,
        DirectStoreOverrides {
            enabled: Some(false),
            holds_scratch_ceiling_bytes: None,
        },
    )
    .await;
    pipeline.live_par2.set_enabled(false);
    let rows_before = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(!rows_before.is_empty(), "phase 1 must have checkpointed");
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a config-disabled gate must redownload the whole job conventionally, got {queued:?}"
    );
    assert!(
        !partial.exists(),
        "mid-flight direct partials must be swept when config turns the gate off"
    );
    assert!(!envelope.exists(), "and so must their envelopes");
    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "and no direct set may survive the restore"
    );
    // Ignored, not deleted: a re-enabled binary can still judge the rows, and it
    // refuses them on the destination probe.
    assert!(
        !pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "a disabled gate must not destroy coverage a re-enabled one could judge"
    );
}

/// D3: a destination that cannot be marked sparse demotes the set, and the
/// refusal happens before the file holds a hole — so nothing it created is left
/// on disk.
#[tokio::test]
async fn a_destination_that_cannot_be_marked_sparse_demotes_before_it_holds_a_hole() {
    use crate::pipeline::direct_store::sparse::SparseMarking;

    let member_name = "Silver.Horizon.S01E53.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 151) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .direct_store
        .set_sparse_marking(SparseMarking::AlwaysFail);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41091);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(SparseMarkFailed)"),
        "a destination that cannot be marked sparse must demote with its own reason, got {shape}"
    );
    assert!(
        !working_dir
            .join(format!("{member_name}.direct.partial"))
            .exists(),
        "the refused destination must not be left behind"
    );
    assert!(
        !working_dir
            .join("silver.horizon.vol00000.envelope")
            .exists(),
        "nor the envelope the same batch would have created"
    );
}

/// The same rule for the holds scratch, which the router creates itself rather
/// than through the destination-preparation seam.
#[tokio::test]
async fn a_holds_scratch_that_cannot_be_marked_sparse_demotes_the_set() {
    use crate::pipeline::direct_store::sparse::SparseMarking;

    let member_name = "Silver.Horizon.S01E54.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline
        .direct_store
        .set_sparse_marking(SparseMarking::AlwaysFail);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(41092);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Article 1 of volume 0 lands before its header, so it has to be held; the
    // 64-byte budget forces the very first hold to page.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchFailed)"),
        "an unmarkable scratch must demote with the scratch's own reason, got {shape}"
    );
    let scratch_files: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".weaver-holds."))
        .collect();
    assert!(
        scratch_files.is_empty(),
        "the scratch it could not mark must not survive, got {scratch_files:?}"
    );
}

// ---------------------------------------------------------------------------
// Encrypted direct-store (plan 136, phase E1)
//
// The spine is the same differential phase 4 established, with one input added:
// the identical job is run with routing on and off *with the same password*, and
// the outputs must be byte-identical. With routing on, no source volume may ever
// appear on disk — which for an encrypted set also means the ciphertext never
// does.
// ---------------------------------------------------------------------------

/// The KDF tuple every encrypted fixture here shares. `lg2 = 4` is 16 PBKDF2
/// rounds: real archives use 2^15 and up, and paying that per fixture would put
/// seconds into the suite for a number nothing under test reads.
const TEST_CRYPT_SALT: [u8; 16] = [0x5A; 16];
const TEST_CRYPT_IV: [u8; 16] = [0xA5; 16];
const TEST_CRYPT_KDF_LG2: u8 = 4;

/// A RAR5 `FHEXTRA_CRYPT` record: `vint(size) || vint(type=1) || body`.
///
/// The body is the format's: version, flags, the KDF count as a raw byte, the
/// 16-byte salt, the 16-byte IV, and — when the flags claim one — the 8-byte
/// password check followed by the first four bytes of its SHA-256, which is the
/// tag the parser validates before it will hand the value to anyone.
fn build_test_rar_crypt_extra(psw_check: Option<&[u8; 8]>, keyed_checksum: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(0));
    let mut flags = 0u64;
    if psw_check.is_some() {
        flags |= 0x0001;
    }
    if keyed_checksum {
        flags |= 0x0002;
    }
    body.extend_from_slice(&encode_test_rar_vint(flags));
    body.push(TEST_CRYPT_KDF_LG2);
    body.extend_from_slice(&TEST_CRYPT_SALT);
    body.extend_from_slice(&TEST_CRYPT_IV);
    if let Some(check) = psw_check {
        body.extend_from_slice(check);
        let digest = <sha2::Sha256 as sha2::Digest>::digest(check);
        body.extend_from_slice(&digest[..4]);
    }
    let type_bytes = encode_test_rar_vint(1);
    let mut record = encode_test_rar_vint((type_bytes.len() + body.len()) as u64);
    record.extend_from_slice(&type_bytes);
    record.extend_from_slice(&body);
    record
}

/// Split points that are deliberately **off** every 16-byte boundary.
///
/// A real `rar -v` split lands wherever the volume filled up, and an encrypted
/// member's parts are not individually block-aligned — only the member's total
/// is. Aligned fixtures would never exercise the straddling block, which is the
/// one shape E-D2's holds exist for.
fn misaligned_parts(total: usize, count: usize) -> Vec<usize> {
    assert!(count >= 1 && total >= count);
    let base = total / count;
    let mut parts = Vec::with_capacity(count);
    let mut used = 0usize;
    for index in 0..count - 1 {
        let len = (base + 1 + index * 3).min(total - used - (count - 1 - index));
        parts.push(len);
        used += len;
    }
    parts.push(total - used);
    assert_eq!(parts.iter().sum::<usize>(), total);
    parts
}

/// One `-m0 -p` stored member split across `volume_count` volumes.
///
/// Mirrors what `rar a -m0 -p<password> -v<size>` writes, which is the recipe
/// `rarpar`'s `tests/fixtures/generate_stored_layout.sh` records: the member's
/// whole plaintext as one AES-256-CBC stream running unbroken across the volume
/// boundaries, `align16(unpacked_size)` cipher bytes in total, one crypt record
/// per part, plain packed CRC32s on the non-final parts and the whole-member
/// checksum on the last.
///
/// - `data_password` encrypts the bytes.
/// - `check_for` is whose password check the headers carry, or `None` for a
///   writer that omitted it. Passing a *different* password here forges a check
///   that admits the wrong one.
/// - `keyed_checksum` sets `FHEXTRA_CRYPT`'s hash-MAC flag on the **final part
///   alone** and folds the whole-member CRC32 with that password's hash key,
///   which is the shape RARLAB `rar` 7.20 writes: only the last part's checksum
///   is the whole member's, so only that one is keyed.
fn encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    check_for: Option<&str>,
    keyed_checksum: bool,
) -> Vec<(String, Vec<u8>)> {
    let material =
        weaver_unrar::derive_rar5_material(data_password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable");
    let key = material.key;
    let hash_key = material.hash_key;
    let psw_check = check_for.map(|password| {
        weaver_unrar::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable")
            .psw_check
    });

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut padded = payload.to_vec();
    padded.resize(cipher_len, 0);
    let cipher = weaver_unrar::test_support::encrypt_aes256_cbc(&key, &TEST_CRYPT_IV, &padded);

    let member_crc = checksum::crc32(payload);
    let member_crc = if keyed_checksum {
        weaver_unrar::convert_crc32_to_mac(member_crc, &hash_key)
    } else {
        member_crc
    };

    let mut offset = 0usize;
    misaligned_parts(cipher_len, volume_count)
        .into_iter()
        .enumerate()
        .map(|(volume, part_len)| {
            let part = &cipher[offset..offset + part_len];
            offset += part_len;
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;

            let mut split_flags = 0u64;
            if !is_first {
                split_flags |= 0x0008;
            }
            if !is_last {
                split_flags |= 0x0010;
            }
            // Layer 1 over cipher bytes, plain: the packed hash on a non-final
            // part covers the packed (= cipher) bytes and `rar` does not key it.
            let data_crc = if is_last {
                member_crc
            } else {
                checksum::crc32(part)
            };
            let extra = build_test_rar_crypt_extra(psw_check.as_ref(), keyed_checksum && is_last);

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_main_header(
                if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                (!is_first).then_some(volume as u64),
            ));
            bytes.extend_from_slice(&build_test_rar_file_header_with_extra(
                member_name,
                split_flags,
                part.len() as u64,
                payload.len() as u64,
                Some(data_crc),
                &extra,
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}
/// What one encrypted job's articles left behind, **without** driving
/// extraction to a terminal state.
///
/// Used where the point is what the router decided. Deliberately not the full
/// gate: a job whose password is wrong never reaches a terminal extraction on
/// *either* path — the archive cannot be opened, so the conventional side sits
/// in `Downloading` exactly as the demoted side does — and a helper that waited
/// for terminality would spend minutes proving it.
struct EncryptedRoutingOutcome {
    /// The direct sets' debug shape, which carries the demotion reason.
    shape: String,
    /// Whether any source volume was materialized, i.e. whether the demotion
    /// handed the bytes to the conventional path rather than dropping them.
    volume_file_seen: bool,
    /// Whether any `.direct.partial` exists.
    partial_seen: bool,
}

async fn encrypted_routing_outcome(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    password: Option<&str>,
) -> EncryptedRoutingOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = password.map(str::to_owned);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let volume_file_seen = volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());
    let partial_seen = std::fs::read_dir(&working_dir)
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".direct.partial")
            })
        })
        .unwrap_or(false);
    EncryptedRoutingOutcome {
        shape,
        volume_file_seen,
        partial_seen,
    }
}

#[tokio::test]
async fn an_encrypted_store_set_routes_plaintext_and_matches_the_conventional_extractor() {
    let member_name = "Silver.Horizon.S02E01.mkv";
    // 3000 is not a multiple of 16, so `cipher_size` is 3008 and the member
    // carries 8 bytes of tail padding: the final block decrypts to plaintext
    // that runs past the member's declared end, and none of it may reach the
    // destination.
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(43001),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43002),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written the encrypted source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "direct routing must never create a source volume file, encrypted or not"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should decrypt the member with the job's password"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the routed member must be plaintext at its final offsets, with no tail padding"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "an encrypted set routed plaintext-once must be byte-identical to the conventional \
         extractor with the same password"
    );
}

#[tokio::test]
async fn a_wrong_password_with_a_check_present_never_routes_a_byte() {
    let member_name = "Silver.Horizon.S02E02.mkv";
    let payload: Vec<u8> = (0..2048u32).map(|index| (index % 199) as u8).collect();
    // The header states the check for the *right* password; the job holds the
    // wrong one, so admission refutes it before a single byte is decrypted.
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let refused =
        encrypted_routing_outcome(JobId(43011), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        refused
            .shape
            .contains("Demoted(EncryptedMemberRefused(WrongPassword))"),
        "a refuted password must be refused at admission, before any byte routes, got {}",
        refused.shape
    );
    assert!(
        !refused.partial_seen,
        "nothing may be written on the strength of a refuted password"
    );
    assert!(
        refused.volume_file_seen,
        "the demotion must hand the volumes to the conventional path, not drop them — which is \
         the parity a wrong password gets: it fails there too, for the same reason"
    );

    // Non-vacuity: the identical set with the right password routes, so the
    // refusal above is a decision about the password and not about the fixture.
    let admitted =
        encrypted_routing_outcome(JobId(43012), &volumes, &arrivals, Some("moonlit-harbour")).await;
    assert!(
        !admitted.shape.contains("Demoted"),
        "the same set with the right password must route, got {}",
        admitted.shape
    );
    assert!(
        !admitted.volume_file_seen,
        "the admitted set must never materialize a source volume"
    );
}

#[tokio::test]
async fn a_wrong_password_with_no_check_routes_until_the_keyed_member_gate_catches_it() {
    let member_name = "Silver.Horizon.S02E03.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    // No password-check value in the header at all, so admission can conclude
    // nothing and routes provisionally. The whole-member checksum is keyed,
    // which makes it the only thing that can catch the wrong password: layer 1's
    // packed hashes cover cipher bytes and pass whatever key was used, so they
    // are wire integrity and not a password test.
    let volumes = encrypted_store_set(member_name, &payload, 3, "moonlit-harbour", None, true);
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(43021), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "a wrong password the header could not refute must be caught by the keyed member gate, \
         got {}",
        caught.shape
    );
    // Deliberately no volume-file assertion here, unlike the check-present
    // case: this gate fires on the *last* article, when the member's plaintext
    // first composes, so the handover has no further articles to ride and the
    // materialization is the demotion machinery's own — already pinned by
    // `a_demoted_set_materializes_its_covered_volumes_instead_of_refetching_them`.

    // Non-vacuity, and the E-D3 claim itself: the identical fixture with the
    // right password verifies through the same keyed fold and completes.
    let clean = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43022),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        clean.member.as_deref(),
        Some(payload.as_slice()),
        "a check-less encrypted member with the right password must still verify and complete"
    );
}

#[tokio::test]
async fn an_encrypted_set_with_no_password_demotes_instead_of_routing_ciphertext() {
    let member_name = "Silver.Horizon.S02E04.mkv";
    let payload: Vec<u8> = (0..1500u32).map(|index| (index % 181) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    // Checklist site 2, as a test: `EncryptedStore` is not `Ineligible`, so the
    // predicate this replaced counted every encrypted member routable and would
    // have sailed past the `routable == 0` demotion with nothing to route.
    let outcome = encrypted_routing_outcome(JobId(43031), &volumes, &arrivals, None).await;
    assert!(
        outcome
            .shape
            .contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "an encrypted set with no password must demote, by name, got {}",
        outcome.shape
    );
    assert!(
        !outcome.partial_seen,
        "a set with no key must not create a destination for ciphertext"
    );
}

#[tokio::test]
async fn an_encrypted_span_that_arrives_before_its_predecessor_block_is_held_then_drained() {
    let member_name = "Silver.Horizon.S02E05.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 173) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    // Every volume's payload article before its header article, and the volumes
    // themselves in reverse order: a span whose 16 preceding cipher bytes live
    // in a volume that has not arrived, in both directions across every part
    // boundary.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32)
        .rev()
        .map(|index| (index, 1))
        .collect();
    arrivals.extend((0..volumes.len() as u32).rev().map(|index| (index, 0)));

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(43041),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43042),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;

    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "a held straddling block must drain to the same plaintext once its other half lands"
    );
    assert!(
        !direct.volume_file_seen,
        "holding a cipher block must not fall back to writing the volume"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "out-of-order encrypted arrival must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_duplicate_encrypted_article_advances_nothing_twice() {
    let member_name = "Silver.Horizon.S02E06.mkv";
    let payload: Vec<u8> = (0..1900u32).map(|index| (index % 167) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    // Every article twice. A duplicate that re-entered the cipher composition
    // would fold the same run into layer 1 twice and fail the part gate; one
    // that re-entered layer 2 would do the same to the keyed member fold.
    let mut arrivals = Vec::new();
    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..2u32 {
            arrivals.push((file_index, segment_number));
            arrivals.push((file_index, segment_number));
        }
    }

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43051),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "duplicate encrypted articles must leave the member exactly once-written"
    );
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn an_encrypted_member_whose_size_is_block_aligned_carries_no_padding() {
    // The other side of the tail-padding case: `unpacked_size % 16 == 0`, so
    // `cipher_size == unpacked_size`, no padding exists and no envelope span is
    // emitted for one. Byte-identical output either way is the assertion —
    // padding arithmetic that was off by a block would show up here as a short
    // member or an over-long one.
    let member_name = "Silver.Horizon.S02E07.mkv";
    let payload: Vec<u8> = (0..2048u32).map(|index| (index % 149) as u8).collect();
    assert_eq!(payload.len() % 16, 0);
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43061),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(direct.member.as_deref(), Some(payload.as_slice()));
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn a_password_that_arrives_after_the_job_was_added_still_admits_the_set() {
    // Plan 136's open question 2, answered: weaver *does* support setting a
    // password after add (`setJobPassword`, and the NZBGet facade's
    // `*Unpack:Password`), both of which mutate the live job spec. The direct
    // sets are built once per job and memoized, so the seam re-reads the spec
    // while any set is still willing to take one.
    let member_name = "Silver.Horizon.S02E08.mkv";
    let payload: Vec<u8> = (0..1700u32).map(|index| (index % 157) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(43071);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The set exists and has taken no password yet — nothing has parsed, so no
    // encrypted member has been classified and nothing has demoted. Reached
    // through the routing seam, which is what admits the sets lazily.
    assert!(
        pipeline
            .direct_route_target(crate::jobs::ids::NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_some(),
        "the set should be admitted and still routing before any password exists"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_demoted()),
        "a set with no password must not demote before it has classified anything"
    );

    // The post-add write both API surfaces perform.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "a password set after add must admit the set and produce the member"
    );
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the set admitted on a post-add password must still route, not materialize"
    );
}

#[tokio::test]
async fn an_encrypted_set_restarted_mid_download_honours_its_floors_and_completes_byte_identically()
{
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S02E09.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(43081);
    // Volume 0 complete, volume 1 half: one file the restore can skip whole and
    // one it must skip only part of — and the part boundary between them is not
    // 16-aligned, so the resumed run has to decrypt at a coverage frontier whose
    // predecessor cipher block is gone with the process that wrote it.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        Some("moonlit-harbour"),
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored encrypted job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that still has the password must re-admit the set, not demote it"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(1, 0)),
        "volume 1's checkpointed articles must not be refetched, got {queued:?}"
    );
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

    for (file_index, segment_number) in queued.clone() {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "the keyed member gate must have re-read the pre-restart plaintext before finalizing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted encrypted set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "a restarted encrypted set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn an_encrypted_set_restarted_without_its_password_demotes_by_name() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S02E10.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(43091);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    // The password is never persisted, by design. A restore that cannot supply
    // one has to demote — by name, and while the set's routed bytes can still
    // materialize its volumes — rather than sit unable to decrypt and unable to
    // give up.
    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        None,
    )
    .await;

    // The layout rebuild runs the same admission the live parse does, so the
    // checkpoint is refused before it can seed anything. That is the "never
    // wedges" half: no seeded coverage means no member left permanently
    // unverifiable, and every article comes back.
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "a set that cannot decrypt must not be left holding seeded coverage it can never re-arm"
    );
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused checkpoint must skip nothing, got {queued:?}"
    );

    // And the "by name" half: the first article to arrive reaches the same
    // admission decision and demotes under its own reason, rather than routing
    // ciphertext or holding forever.
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "a restored encrypted set with no password must demote by name, got {shape}"
    );
}

// ---------------------------------------------------------------------------
// E-D5: the machinery plan 136 changes nothing about, asserted rather than
// assumed. Each of these is the encrypted twin of a plan 135 guarantee.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn encrypted_routing_leaves_the_plan_135_guarantees_untouched() {
    let member_name = "Silver.Horizon.S02E11.mkv";
    let payload: Vec<u8> = (0..5000u32).map(|index| (index % 227) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(43101);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Payload before headers, so the holds machinery carries cipher bytes the
    // layout cannot place yet — the same hold plan 135 uses, over ciphertext.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));
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

    // D7 suppression: an encrypted set's source volumes are direct volumes, so
    // no legacy floor, completed-file row or archive re-probe may be written.
    assert!(
        (0..volumes.len() as u32)
            .all(|file_index| pipeline.is_direct_source_file(NzbFileId { job_id, file_index })),
        "every encrypted source volume must be suppressed as a direct volume"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "an encrypted direct set must not enter the archive topology"
    );

    // Coverage floors and barriers: the barrier runs and publishes floors over
    // the *source* volumes, which are cipher space — untouched by the write
    // transform, because the transform happens on the destination side.
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the encrypted set must still be routing");
    assert!(!set.is_demoted(), "no gate here may demote a clean set");
    // Non-vacuity for everything below: this set really is decrypting at write
    // time, so these are the encrypted twins of the plan 135 guarantees rather
    // than the plaintext originals under a new name.
    assert!(
        set.router.routes_encrypted(),
        "the fixture must have admitted an encrypted member"
    );
    assert!(
        (0..volumes.len() as u32).all(|volume_index| set.volume_coverage(volume_index).is_empty()
            || set.volume_coverage(volume_index).contiguous_from_zero() > 0),
        "every touched volume must have a contiguous floor in source (cipher) space"
    );

    // The sweep and finalization wait: the job still finishes, in one place,
    // byte-identical.
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(member.as_deref(), Some(payload.as_slice()));
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "the sweep must not have left a source volume behind"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_set_demotes_before_the_authoritative_pass() {
    // The one thing an encrypted set cannot yet do (plan 136 defers it to phase
    // E2): its destinations hold plaintext while PAR2 describes the posted
    // cipher, so a set served to the authoritative pass would have every slice
    // reported damaged and the repairer handed a virtual volume that was never
    // broken. It demotes when the recovery set appears — which can be at any
    // point in a job's life — and finishes conventionally.
    let member_name = "Silver.Horizon.S02E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.live_par2.set_enabled(false);
    let job_id = JobId(43111);
    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    // Non-vacuity: it really did route the encrypted set before the PAR2 index
    // turned up, so the demotion below is the guard firing and not admission.
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "the encrypted set must have been routing before the recovery set arrived"
    );

    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(EncryptedPar2Unsupported)"),
        "a PAR2-bearing encrypted set must demote before the authoritative pass, got {shape}"
    );

    // The demotion refetches rather than reconstructs, which is the other half
    // of the same rule: the member partials hold plaintext, so materializing a
    // volume out of them would write decrypted bytes where PAR2 expects cipher.
    // Every article comes back, and no volume was fabricated in the meantime.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.is_empty(),
        "an encrypted set that demotes must refetch its volumes rather than materialize them"
    );
    assert!(
        volumes.iter().all(|(filename, _)| working_dir
            .join(filename)
            .metadata()
            .map(|meta| meta.len())
            .unwrap_or(0)
            == 0),
        "no source volume may have been reconstructed out of decrypted destinations"
    );
    let _ = &complete_dir;
}
