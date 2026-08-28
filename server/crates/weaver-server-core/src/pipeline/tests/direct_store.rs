//! Direct-store routing.
//!
//! The spine is differential: the identical job gate is run with routing on and
//! off, and the outputs must be byte-identical. With routing on, no source
//! volume may ever appear on disk.

use super::*;

use std::path::Path;

use crate::pipeline::direct_store::DirectStoreGate;
use crate::pipeline::direct_store::router::DemotionReason;
use crate::pipeline::direct_store::wiring::MAX_DIRECT_REPAIR_DEFER_WAVES;

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
/// is exactly what the two integrity layers read.
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

/// A job's **payload root**: `complete/.weaver-staging/<job_id>` under the
/// harness's temp dir.
///
/// Direct-store member payload is born here rather than in the working
/// directory, so completion publishes it by a same-volume rename exactly as it
/// publishes a member the incremental extractor produced. Mirrors
/// `Pipeline::deterministic_extraction_staging_dir` against the layout
/// `new_direct_pipeline_with` configures.
fn payload_root(temp_dir: &TempDir, job_id: JobId) -> PathBuf {
    temp_dir
        .path()
        .join("complete")
        .join(".weaver-staging")
        .join(job_id.0.to_string())
}

/// The `.direct.partial` one member routes into, under the payload root.
fn direct_partial(temp_dir: &TempDir, job_id: JobId, member_name: &str) -> PathBuf {
    payload_root(temp_dir, job_id).join(format!("{member_name}.f0.direct.partial"))
}

/// A member sitting **unpublished** in the job's staging root.
///
/// The third place a finished member can legitimately be, and the one this
/// harness gained when direct-store started writing payload onto the complete
/// volume: `complete_dir/.weaver-staging/<job_id>/<member>` is where both direct
/// finalization and the incremental extractor leave a member until the final
/// move renames it into the output directory. The job id is wildcarded because
/// one harness temp dir only ever runs one job.
fn staging_member(complete_dir: &Path, member_name: &str) -> Option<Vec<u8>> {
    std::fs::read_dir(complete_dir.join(".weaver-staging"))
        .ok()?
        .flatten()
        .find_map(|entry| std::fs::read(entry.path().join(member_name)).ok())
}

/// Whether any `.direct.partial` is left at the top level of `root`.
fn any_direct_partial(root: &Path) -> bool {
    std::fs::read_dir(root)
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".direct.partial")
            })
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Quick Open: the cache is a hint, and direct-store never routes on one
// ---------------------------------------------------------------------------

/// The RAR5 extra record that points a main header at a `QO` block: a LOCATOR
/// record carrying the quick-open offset **relative to the main header itself**.
fn build_test_rar_locator_extra(qopen_offset_from_main: u64) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&encode_test_rar_vint(0x01)); // LOCATOR.
    record.extend_from_slice(&encode_test_rar_vint(0x01)); // LOCATOR_QLIST.
    record.extend_from_slice(&encode_test_rar_vint(qopen_offset_from_main));
    let mut out = encode_test_rar_vint(record.len() as u64);
    out.extend_from_slice(&record);
    out
}

/// One cached header inside a `QO` block: `crc32(size || body) || size || body`,
/// where the body is the header's offset *back* from the `QO` header, its
/// length, and the header bytes themselves.
fn build_test_rar_qopen_record(
    qopen_header_offset: u64,
    original_header_offset: u64,
    cached_header: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(0)); // flags, unused here.
    body.extend_from_slice(&encode_test_rar_vint(
        qopen_header_offset - original_header_offset,
    ));
    body.extend_from_slice(&encode_test_rar_vint(cached_header.len() as u64));
    body.extend_from_slice(cached_header);

    let size = encode_test_rar_vint(body.len() as u64);
    let crc = checksum::crc32(&[size.as_slice(), body.as_slice()].concat());

    let mut out = Vec::new();
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&size);
    out.extend_from_slice(&body);
    out
}

/// Where the `QO` block starts in the last volume. Fixed rather than derived,
/// because the locator that names it lives in the main header *before* it and
/// its own vint width would otherwise depend on the answer.
const QOPEN_OFFSET: u64 = 512;

/// A two-volume stored set whose **last** volume carries a locator and a `QO`
/// cache, past its end-of-archive record where a real archiver puts one.
///
/// `forged_name` is the whole point. `None` builds an honest cache that echoes
/// exactly the header the physical walk finds. `Some(name)` appends a second
/// cached file header that no physical header describes — the shape the RAR
/// spec warns can be crafted, and the one direct-store forbids routing a byte
/// on.
fn quick_open_store_set(
    member_name: &str,
    payload: &[u8],
    forged_name: Option<&str>,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let split = payload.len() / 2;

    let mut first = Vec::new();
    first.extend_from_slice(&TEST_RAR5_SIG);
    first.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    first.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0x0010,
        split as u64,
        payload.len() as u64,
        Some(checksum::crc32(&payload[..split])),
    ));
    first.extend_from_slice(&payload[..split]);
    first.extend_from_slice(&build_test_rar_end_header(true));

    let main = {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(0x0001 | 0x0002));
        type_body.extend_from_slice(&encode_test_rar_vint(1));
        build_test_rar_header(
            1,
            0,
            &type_body,
            &build_test_rar_locator_extra(QOPEN_OFFSET - TEST_RAR5_SIG.len() as u64),
        )
    };
    let real_header = build_test_rar_file_header(
        member_name,
        0x0008,
        (payload.len() - split) as u64,
        payload.len() as u64,
        Some(member_crc),
    );
    let real_header_offset = (TEST_RAR5_SIG.len() + main.len()) as u64;

    let mut second = Vec::new();
    second.extend_from_slice(&TEST_RAR5_SIG);
    second.extend_from_slice(&main);
    second.extend_from_slice(&real_header);
    second.extend_from_slice(&payload[split..]);
    second.extend_from_slice(&build_test_rar_end_header(false));
    assert!(
        second.len() as u64 <= QOPEN_OFFSET,
        "the physical headers must end before the QO block"
    );
    second.resize(QOPEN_OFFSET as usize, 0);

    let mut records = build_test_rar_qopen_record(QOPEN_OFFSET, real_header_offset, &real_header);
    if let Some(forged_name) = forged_name {
        // Cached at an offset inside the padding, so nothing physical sits
        // there: the only thing that says this member exists is the cache.
        records.extend_from_slice(&build_test_rar_qopen_record(
            QOPEN_OFFSET,
            QOPEN_OFFSET - 64,
            &build_test_rar_file_header(forged_name, 0, 16, 16, Some(0)),
        ));
    }
    records.extend_from_slice(&build_test_rar_qopen_record(
        QOPEN_OFFSET,
        QOPEN_OFFSET - 32,
        &build_test_rar_end_header(false),
    ));
    second.extend_from_slice(&build_test_rar_service_header("QO", records.len() as u64));
    second.extend_from_slice(&records);

    vec![
        ("silver.horizon.part01.rar".to_string(), first),
        ("silver.horizon.part02.rar".to_string(), second),
    ]
}

/// The member names the library reports for a volume under its **default**
/// options, which consult the Quick Open cache.
fn library_default_member_names(volume: &[u8]) -> Vec<String> {
    unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(volume.to_vec()), None)
        .expect("the fixture volume parses")
        .members
        .into_iter()
        .map(|member| member.name)
        .collect()
}

#[tokio::test]
async fn a_forged_quick_open_member_never_reaches_the_router() {
    let member_name = "Silver.Horizon.S01E61.mkv";
    let forged_name = "Silver.Horizon.S01E61.forged";
    let payload: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    let volumes = quick_open_store_set(member_name, &payload, Some(forged_name));

    // Non-vacuity, stated against the library rather than against weaver: with
    // Quick Open left on — which is `parse_all_headers`' default and what
    // `parse_volume_facts` still uses — the forged member *is* reported, and
    // `members_extend` would have adopted it and routed payload into it.
    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![member_name.to_string(), forged_name.to_string()],
        "the fixture must actually forge a member the physical walk cannot see"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, _working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41101), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(QuickOpenMismatch)"),
        "a volume whose headers disagree with its physical walk must leave direct mode \
         under its own reason, got {shape}"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41101), forged_name).exists(),
        "no destination may be created for a member only the Quick Open cache claims"
    );
}

#[tokio::test]
async fn an_honest_quick_open_cache_still_routes() {
    let member_name = "Silver.Horizon.S01E62.mkv";
    let payload: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    let volumes = quick_open_store_set(member_name, &payload, None);

    // The other half of the non-vacuity: the same fixture, same locator, same
    // `QO` block — and the cache agrees with the walk, so nothing is refused.
    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![member_name.to_string()],
        "an honest cache reports exactly the physical member"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, _) =
        run_direct_store_routing_only(&temp_dir, JobId(41102), &volumes, &arrivals).await;
    assert!(
        !shape.contains("Demoted"),
        "the cross-check must refuse a *disagreement*, not the presence of a cache, got {shape}"
    );
}

/// The decoded extent of one article, for a volume cut into `articles` equal
/// pieces. At `articles == 2` this is the head/tail split every fixture uses.
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

/// The gate runner with one extra input: the job's password.
///
/// Everything else is the original harness unchanged, deliberately — the whole
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
    let staged = staging_member(&complete_dir, member_name);
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    assert!(
        completed.is_none() || (staged.is_none() && left_behind.is_none()),
        "a finished member must exist in exactly one place"
    );
    let (member, member_location) = match (completed, staged, left_behind) {
        (Some(bytes), _, _) => (Some(bytes), Some("complete")),
        (None, Some(bytes), _) => (Some(bytes), Some("staging")),
        (None, None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None, None) => (None, None),
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
async fn a_clean_three_article_member_stays_virtual_and_routes_once() {
    let member_name = "Silver.Horizon.S01E04.Clean.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(410051);

    let (mut pipeline, working_dir) =
        route_articles_as_dispatched(&temp_dir, job_id, &volumes, 3).await;

    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(job_id)).contains("Finalized"),
        "the clean twin must finish without demoting"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "clean direct routing must not materialize either source volume"
    );
    assert_eq!(
        std::fs::read(payload_root(&temp_dir, job_id).join(member_name))
            .ok()
            .as_deref(),
        Some(payload.as_slice()),
        "the clean twin must retain the exact routed member"
    );
    assert_eq!(queued_segments(&mut pipeline, job_id), Vec::new());
}

#[tokio::test]
async fn member_checksum_demotion_hands_the_live_tail_to_a_reconstructed_prefix() {
    let member_name = "Silver.Horizon.S01E04.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 173) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 2);

    // The final volume's header closes the member chain before its payload has
    // arrived. Corrupt a payload byte in article three so the fused member CRC
    // refuses that article inside `route`, before direct ownership transfers.
    let end_header_len = build_test_rar_end_header(false).len();
    let corrupt_at = volumes[1].1.len() - end_header_len - 1;
    assert!(
        corrupt_at >= article_extent(volumes[1].1.len(), 2, 3).0,
        "the corruption must live in the triggering article"
    );
    volumes[1].1[corrupt_at] ^= 0xFF;

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41005);
    let (mut pipeline, working_dir) =
        route_articles_as_dispatched(&temp_dir, job_id, &volumes, 3).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)"),
        "the whole-member gate should have demoted the set, got {shape}"
    );
    for (filename, posted) in &volumes {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(posted.as_slice()),
            "reconstruction plus the in-hand final article must reproduce {filename}"
        );
    }
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        (0..volumes.len() as u32).all(|file_index| {
            state
                .assembly
                .file(NzbFileId { job_id, file_index })
                .is_some_and(|file| file.is_complete())
        }),
        "conventional assembly must own and complete both demoted volumes"
    );
    let (tail_start, tail_end) = article_extent(volumes[1].1.len(), 2, 3);
    assert_eq!(
        state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 1,
            })
            .and_then(|file| file.placement_of(2)),
        Some((tail_start as u64, (tail_end - tail_start) as u32)),
        "the in-hand article must restore the placement erased by demotion reset"
    );
    assert_eq!(
        state.downloaded_bytes,
        volumes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>(),
        "the triggering article remains counted exactly once"
    );
    assert!(
        !pipeline.write_buffers.contains_key(&NzbFileId {
            job_id,
            file_index: 0,
        }),
        "a fully reconstructed volume must not retain an empty write buffer"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        Vec::<(u32, u32)>::new(),
        "no article, especially the triggering one, should be refetched"
    );
    assert!(
        !pipeline
            .pending_retries_by_segment
            .contains_key(&SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 2,
            }),
        "the triggering article must not enter delayed retry state"
    );
    assert!(
        !payload_root(&temp_dir, job_id).join(member_name).exists(),
        "a member failing its whole-member gate must not be committed as if it passed"
    );
    assert!(
        !direct_partial(&temp_dir, job_id, member_name).exists(),
        "demotion must delete the set's partial direct output"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0,
        "durable conventional completion must clear the demotion gate"
    );
}

// ---------------------------------------------------------------------------
// The confirming parse
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

/// The first shape's
/// `direct_store_refuses_a_set_whose_last_volume_hides_a_second_member`,
/// upgraded: the hidden member is now **adopted** rather than demoting the set.
///
/// The first shape had two reasons to demote here — the layout refused the
/// re-add, and even if it had not, a second routable member was out of scope.
/// Both are gone now: the router rebuilds its layout from every volume's newest
/// facts when a longer prefix reveals a header, and routes as many members as
/// the archive has.
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
            std::fs::read(
                crate::pipeline::Pipeline::member_output_paths(
                    &payload_root(&temp_dir, JobId(41010)),
                    name
                )
                .0
            )
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
// PAR2 over virtual volumes
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
    let file_index = append_par2_index(&mut spec, par2_bytes);
    (spec, file_index)
}

/// Appends the index as one more downloadable file and returns its NZB index.
///
/// Split out of [`par2_bearing_job_spec`] so a spec built with a different
/// article count — the restart harness's — can carry one too.
fn append_par2_index(spec: &mut JobSpec, par2_bytes: &[u8]) -> u32 {
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
    file_index
}

#[tokio::test]
async fn demotion_materialization_gate_is_scoped_to_its_bound_par2_set() {
    let first_member = "Silver.Horizon.S01E30.mkv";
    let second_member = "Amber.Sky.S01E30.mkv";
    let first_payload: Vec<u8> = (0..1800u32).map(|index| (index % 173) as u8).collect();
    let second_payload: Vec<u8> = (0..1800u32).map(|index| (index % 181) as u8).collect();
    let first = single_member_store_set(first_member, &first_payload, 2);
    let second: Vec<(String, Vec<u8>)> = single_member_store_set(second_member, &second_payload, 2)
        .into_iter()
        .map(|(filename, bytes)| (filename.replace("silver.horizon", "amber.sky"), bytes))
        .collect();
    let first_par2 = par2_index_over_volumes(&first);
    let second_par2 = par2_index_over_volumes(&second);
    let mut volumes = first.clone();
    volumes.extend(second.clone());

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41058);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Two direct sets", &volumes);
    let first_index = append_par2_index(&mut spec, &first_par2);
    let second_index = spec.files.len() as u32;
    let second_index_name = "amber.sky.par2".to_string();
    spec.total_bytes += u64::from(yenc_declared_bytes(second_par2.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&second_index_name),
        filename: second_index_name,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(second_par2.len() as u32),
            message_id: "second-direct-par2-index@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, first_index, &first_par2).await;
    deliver_par2_index(&mut pipeline, job_id, second_index, &second_par2).await;

    take_queued_segment(
        &mut pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
    );
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let first_set_index = pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .position(|set| {
            set.plan()
                .volumes
                .values()
                .any(|file_index| *file_index == 0)
        })
        .expect("the first archive set was admitted");
    std::fs::remove_file(working_dir.join("silver.horizon.f0.vol00000.envelope")).unwrap();
    pipeline
        .demote_direct_set(
            job_id,
            first_set_index,
            DemotionReason::MemberChecksumMismatch,
        )
        .await;

    let first_set_id = pipeline
        .resolve_par2_file_binding(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("first volume binding")
        .recovery_set_id;
    let second_set_id = pipeline
        .resolve_par2_file_binding(NzbFileId {
            job_id,
            file_index: first.len() as u32,
        })
        .expect("second volume binding")
        .recovery_set_id;
    assert_ne!(first_set_id, second_set_id);
    assert_eq!(pipeline.par2_served_set_id(job_id), Some(first_set_id));
    let _drained = queued_segments(&mut pipeline, job_id);
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, verifies_before,
        "the shared completion funnel must stop before starting PAR2"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        first.len(),
        "the first set remains pending while its rescued articles are queued"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, first_set_id),
        "the demoted set's own PAR2 verdict must wait"
    );
    let original_identity = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .insert(
            0,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: first[0].0.clone(),
                current_filename: first[0].0.clone(),
                canonical_filename: Some(first[1].0.clone()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        );
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_none(),
        "the conflicting canonical candidate must make this pending set unresolved"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, second_set_id),
        "an unresolved pending set must conservatively block the served set"
    );
    if let Some(identity) = original_identity {
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .file_identities
            .insert(0, identity);
    } else {
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .file_identities
            .remove(&0);
    }
    assert!(
        pipeline.demoted_materializations_ready_for_par2(job_id, second_set_id),
        "an unrelated bound recovery set must not wait behind the demotion"
    );
    pipeline.clear_par2_runtime_state(job_id);
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0,
        "the shared cancellation/failure/teardown seam must clear the gate"
    );
}

/// What one par2-bearing job gate produced.
#[derive(Debug)]
struct Par2GateOutcome {
    member: Option<Vec<u8>>,
    member_location: Option<&'static str>,
    status: Option<JobStatus>,
    volume_file_seen: bool,
    admitted: bool,
    authoritative_verify_calls: usize,
    /// Whether *some* pass reached a genuine PAR2 verdict for this job — the
    /// conventional authoritative pass, the direct session short-circuit, or
    /// the direct quiet pass's own read. The three used to be interchangeable
    /// non-vacuity evidence because a clean direct verdict always fell
    /// through to a conventional whole-set read anyway; now that the direct
    /// gate settles a clean verdict itself instead of asking the whole-set
    /// pass to reach the same answer again, `authoritative_verify_calls`
    /// alone no longer proves a par2-bearing direct job did its job.
    verdict_reached: bool,
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
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
) -> Par2GateOutcome {
    run_par2_direct_gate_with_password(gate, job_id, member_name, volumes, None).await
}

/// [`run_par2_direct_gate`] with one extra input, so an encrypted
/// set's par2-bearing differential runs through exactly the same code the
/// plaintext one does.
async fn run_par2_direct_gate_with_password(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    password: Option<&str>,
) -> Par2GateOutcome {
    let par2_bytes = par2_index_over_volumes(volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", volumes, &par2_bytes);
    spec.password = password.map(str::to_owned);
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

    // The harness supplies decoded articles directly.  Once they have all
    // arrived, model the dispatcher's exhausted discovery state and let the
    // completion gate observe it.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;

    // Snapshotted here, not at the end: the exhausted download pass runs the
    // verification, and a job that then completes has its direct-store runtime
    // pruned, so the sets are gone by the time extraction is terminal.
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
    let staged = staging_member(&complete_dir, member_name);
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    let (member, member_location) = match (completed, staged, left_behind) {
        (Some(bytes), _, _) => (Some(bytes), Some("complete")),
        (None, Some(bytes), _) => (Some(bytes), Some("staging")),
        (None, None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None, None) => (None, None),
    };
    Par2GateOutcome {
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
        volume_file_seen,
        admitted,
        authoritative_verify_calls: pipeline.par2_authoritative_verify_calls,
        verdict_reached: pipeline.par2_authoritative_verify_calls > 0
            || pipeline.direct_session_pass_calls > 0
            || !pipeline.direct_verify_read_splits.is_empty(),
        demotions: sets_after_verification,
    }
}

#[tokio::test]
async fn a_par2_bearing_direct_job_completes_byte_identically_and_never_writes_a_volume() {
    // An earlier shape refused this job at admission
    // (`direct_store.refused.par2_present`) because every PAR2 path read the
    // volume files routing never creates. The `FileAccess` adapter answers
    // those reads out of the envelope plus the routed member bytes, so the
    // refusal is gone and this test states the new behaviour: the set routes,
    // the job verifies against *virtual* volumes, and the output is what the
    // conventional extractor would have produced.
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let conventional = run_par2_direct_gate(
        DirectStoreGate::Disabled,
        JobId(41011),
        member_name,
        &volumes,
    )
    .await;
    let direct = run_par2_direct_gate(
        DirectStoreGate::Enabled,
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
    // The point: verification finishes with the download. The live
    // short-circuit may have fired, or the direct quiet pass may have reached
    // and settled a clean verdict on its own, or the conventional
    // authoritative pass may have run against the virtual volumes — all three
    // are wave-2 behaviour, and a job that took none of them would have
    // failed the byte comparison above against zero files.
    assert!(
        direct.verdict_reached,
        "the job must have reached a PAR2 verdict; authoritative={}",
        direct.authoritative_verify_calls
    );
}

/// The same volume bytes under hex names that classify to nothing — the shape
/// of a fully obfuscated posting. The real names survive only inside the PAR2
/// descriptions the caller builds from the un-obfuscated list.
fn obfuscate_volumes(volumes: &[(String, Vec<u8>)]) -> Vec<(String, Vec<u8>)> {
    volumes
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                bytes.clone(),
            )
        })
        .collect()
}

/// Runs one whole **obfuscated** par2-bearing job gate.
///
/// The spec's filenames are hex, so `DirectSetPlan::discover` finds nothing
/// and any admission must come from the PAR2 descriptions, which carry the
/// real names. `par2_first` decides whether the index arrives before any
/// volume article — the identity window — or after them all, which is too
/// late by construction: every volume's bytes have already landed
/// conventionally.
async fn run_obfuscated_par2_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_first: bool,
    arrivals: &[(u32, u32)],
) -> Par2GateOutcome {
    let par2_bytes = par2_index_over_volumes(volumes);
    let obfuscated = obfuscate_volumes(volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    spec.password = None;
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    if par2_first {
        deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;
    }
    let mut volume_file_seen = false;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            *file_index,
            *segment_number,
        )
        .await;
        for (filename, _) in &obfuscated {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    let admitted = !pipeline.direct_store.sets_for(job_id).is_empty();
    if !par2_first {
        deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;
    }

    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    let sets_after_verification = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    // Both name populations: a conventional volume is born under its hex name
    // and renamed to its described name once identity settles, and either
    // sighting is a volume file this gate produced.
    let volume_file_at_end = obfuscated
        .iter()
        .chain(volumes.iter())
        .any(|(filename, _)| working_dir.join(filename).exists());
    let volume_file_seen = volume_file_seen || volume_file_at_end;

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let completed = std::fs::read(output_root.join(member_name)).ok();
    let staged = staging_member(&complete_dir, member_name);
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    let (member, member_location) = match (completed, staged, left_behind) {
        (Some(bytes), _, _) => (Some(bytes), Some("complete")),
        (None, Some(bytes), _) => (Some(bytes), Some("staging")),
        (None, None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None, None) => (None, None),
    };
    Par2GateOutcome {
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
        volume_file_seen,
        admitted,
        authoritative_verify_calls: pipeline.par2_authoritative_verify_calls,
        verdict_reached: pipeline.par2_authoritative_verify_calls > 0
            || pipeline.direct_session_pass_calls > 0
            || !pipeline.direct_verify_read_splits.is_empty(),
        demotions: sets_after_verification,
    }
}

#[tokio::test]
async fn an_obfuscated_rar_set_admits_from_par2_identity_and_never_writes_a_volume() {
    // The defect this states the fix for: a job whose files are all hex names
    // carries no `RarVolume` role, so spec discovery admits nothing and the
    // examined latch used to settle that forever. With the index parsed before
    // the first data article — the promoted-metadata shape — the descriptions
    // supply the roster and every volume binds by its own first bytes.
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_obfuscated_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41090),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;
    let direct = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41091),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        direct.admitted,
        "an obfuscated set must admit from PAR2 identity, got sets {}",
        direct.demotions
    );
    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "an identity-admitted job must never create a source volume file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate should reproduce the member payload"
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
        "the identity-admitted job must produce the conventional gate's output; sets = {}",
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
}

#[tokio::test]
async fn identity_admission_refuses_once_volume_bytes_landed_conventionally() {
    // The index arriving after the data is the boundary of the roster rung:
    // every volume already has conventional bytes on disk, so no binding may
    // be made — the envelope model owns all of a routed volume's bytes or
    // none of them. RAR4 volumes, deliberately: a RAR5 set would be admitted
    // by the header rung before the index ever mattered, which is the better
    // outcome but not the boundary under test. The job must simply complete
    // the way it does today, with no admission and no wedge.
    let member_name = "Silver.Horizon.S01E09.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_rar4_store_set_numbered(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let outcome = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41092),
        member_name,
        &volumes,
        false,
        &arrivals,
    )
    .await;

    assert!(
        !outcome.admitted,
        "identity admission must refuse once volume bytes landed, got {}",
        outcome.demotions
    );
    assert!(
        outcome.volume_file_seen,
        "the volumes should have been written conventionally"
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional fallback must still produce the member; status {:?}",
        outcome.status
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "the job should have completed conventionally, got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn arming_schedules_a_probe_wave_ahead_of_candidate_payload() {
    // The dispatch half of identity admission: an obfuscated post's NZB order
    // scrambles the volume order, and streaming payload in that order piles
    // unplaceable member bytes into holds until the scratch ceiling demotes
    // the set. Arming therefore pulls every candidate's first article to the
    // front of the queue — binding every file within a few round trips — and
    // each binding then re-ranks its file to the name path's own
    // `10 + volume` priority, restoring in-order volume streaming.
    let member_name = "Silver.Horizon.S01E19.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 163) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41101);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;

    // The probe wave: after the PAR2 index (priority 0, still queued — the
    // harness delivered its bytes directly), the next three dispatches are
    // the three candidates' first articles, in file order, ahead of every
    // payload article.
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let index_work = state.download_queue.pop().expect("queued index work");
    assert_eq!(
        index_work.segment_id.file_id.file_index, index_file_index,
        "the declared index leads the queue"
    );
    for expected_file in 0..3u32 {
        let work = state.download_queue.pop().expect("queued work");
        assert_eq!(
            (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number
            ),
            (expected_file, 0),
            "the probe wave must lead the queue"
        );
        assert!(
            work.completion_critical,
            "the probe wave must stay ahead of completion-critical PAR2 work"
        );
    }

    // A binding re-ranks its file to the name path's volume priority.
    submit_volume_article(&mut pipeline, job_id, &obfuscated, 0, 0).await;
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the probe article should have admitted the set"
    );
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let repriced = state
        .download_queue
        .peek_next_matching(|work| work.segment_id.file_id.file_index == 0)
        .map(|work| work.priority);
    assert_eq!(
        repriced,
        Some(10),
        "the bound file's payload must carry its volume's name-path priority"
    );
}

#[tokio::test]
async fn an_out_of_order_payload_article_no_longer_poisons_the_binding() {
    // The production shape that killed the first cut of this seam: on a wide
    // connection pool a file's later article routinely decodes before its
    // offset-zero article. The conventional path parks it in the write
    // reorder buffer — in memory, unwritten — so when the offset-zero
    // article then establishes the binding, the parked article is reclaimed
    // into the routed volume and the set streams whole. Nothing leaks, and
    // nothing demotes.
    let member_name = "Silver.Horizon.S01E10.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 193) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Volume 1's payload article arrives before its offset-zero article, and
    // volume 0's pair is inverted too, so both the admitting binding and a
    // later one exercise the reclaim.
    let arrivals = [(0, 1), (0, 0), (1, 1), (1, 0), (2, 0), (2, 1)];

    let outcome = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41093),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        outcome.admitted,
        "the set must admit despite out-of-order arrivals, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "reclaimed articles must route, never materialize a volume, got {}",
        outcome.demotions
    );
    assert!(
        outcome.demotions.contains("Finalized"),
        "the set should have finalized, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the reclaimed set must reproduce the member payload; status {:?}",
        outcome.status
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn an_unmatched_obfuscated_extra_stays_conventional_beside_an_identity_set() {
    // A junk extra — the nfo, the sample — matches no description. Its
    // offset-zero evaluation settles it as a non-member, it streams
    // conventionally, and the identity set is unaffected.
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);
    let junk: Vec<u8> = (0..20_000u32).map(|index| (index % 251) as u8).collect();

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41094);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    let junk_index = spec.files.len() as u32;
    let junk_name = format!("{:032x}", 0xd1c7_ffff_u128);
    spec.total_bytes += u64::from(yenc_declared_bytes(junk.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&junk_name),
        filename: junk_name.clone(),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(junk.len() as u32),
            message_id: "obfuscated-junk@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: junk_index,
        },
        0,
        0,
        &junk,
        &junk_name,
        None,
    )
    .await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the identity set must admit despite the unmatched extra"
    );

    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    assert!(
        sets.contains("Finalized"),
        "the set should have finalized with the extra beside it, got {sets}"
    );
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let member = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| staging_member(&complete_dir, member_name));
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the identity set must produce the member beside the extra"
    );
    assert!(
        !obfuscated
            .iter()
            .chain(volumes.iter())
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "no source volume file may exist for the finalized identity set"
    );
}

/// [`single_member_rar4_store_set`] with **numbered** end-of-archive records
/// — the shape WinRAR's new-numbering era actually writes. The unnumbered
/// variant is kept for the paths that must tolerate it; a renamed
/// conventional set needs the numbers, because every unnumbered volume's
/// parsed facts claim position zero and the fact-driven topology collides.
fn single_member_rar4_store_set_numbered(
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
            bytes.extend_from_slice(&build_test_rar4_end_header_numbered(
                !is_last,
                volume as u16,
            ));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn a_restored_job_with_a_conventional_floor_is_never_readmitted() {
    // The restart tear: restore rebuilds a conventional floor and commits the
    // skipped segments into the assembly, so the file on disk owns the
    // volume's prefix. Admitting the set at the next decoded segment would
    // route every remaining article into an envelope instead — the two
    // halves would never meet, and extraction would walk real headers into
    // a hole. The set must stay on the path that owns its bytes.
    let member_name = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 157) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 1);
    let (volume_name, volume_bytes) = &volumes[0];
    // The spec's own first-article boundary, so the restored floor covers
    // exactly segment 0 and the run owes exactly segment 1.
    let (_, split_at) = article_extent(volume_bytes.len(), 0, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41102);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = temp_dir.path().join("restored-floor");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    // The prefix the previous run wrote conventionally, exactly as the
    // restart sweep would find it. The restore floor counts the spec's
    // yEnc-encoded segment sizes and clamps to the on-disk length, so the
    // file is extended to the encoded floor — the extension sits in the
    // segment-1 region, which the restored run rewrites below.
    let encoded_floor = u64::from(yenc_declared_bytes(split_at as u32));
    tokio::fs::write(working_dir.join(volume_name), &volume_bytes[..split_at])
        .await
        .unwrap();
    let partial = tokio::fs::File::options()
        .write(true)
        .open(working_dir.join(volume_name))
        .await
        .unwrap();
    partial.set_len(encoded_floor).await.unwrap();
    drop(partial);

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::from([(0u32, encoded_floor)]),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    // The remaining article decodes on the restored run.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        1,
        split_at as u64,
        &volume_bytes[split_at..],
        volume_name,
        None,
    )
    .await;

    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "a set whose volume already has conventional bytes must not admit, got {:?}",
        pipeline.direct_store.sets_for(job_id)
    );
    // A restored file's reorder cursor starts at zero with its skipped
    // segments never arriving, so the tail segment sits buffered until the
    // quiescent flusher (or backlog pressure) writes it at its own offset —
    // the same drain a real restored run relies on. The harness supplied the
    // article directly, so the queued copy is cleared first, exactly as the
    // other whole-job gates model an exhausted dispatcher.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.flush_quiescent_write_backlog().await;
    let on_disk = std::fs::read(working_dir.join(volume_name)).unwrap();
    let first_diff = on_disk
        .iter()
        .zip(volume_bytes.iter())
        .position(|(a, b)| a != b);
    assert!(
        on_disk == *volume_bytes,
        "the restored file must receive the remaining bytes and stay whole          (on_disk_len={} expected_len={} first_diff={:?} split_at={split_at})",
        on_disk.len(),
        volume_bytes.len(),
        first_diff,
    );
}

/// Runs one whole **par2-less obfuscated** job gate: hex names, no index
/// anywhere, so the only admissible evidence is the volumes' own RAR5
/// headers.
async fn run_obfuscated_headers_gate(
    job_id: JobId,
    member_name: &str,
    obfuscated: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> Par2GateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = direct_store_job_spec("Silver Horizon", obfuscated);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            obfuscated,
            *file_index,
            *segment_number,
        )
        .await;
        for (filename, _) in obfuscated {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    let admitted = !pipeline.direct_store.sets_for(job_id).is_empty();
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let volume_file_seen = volume_file_seen
        || obfuscated
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists());
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let completed = std::fs::read(output_root.join(member_name)).ok();
    let staged = staging_member(&complete_dir, member_name);
    let left_behind = std::fs::read(working_dir.join(member_name)).ok();
    let (member, member_location) = match (completed, staged, left_behind) {
        (Some(bytes), _, _) => (Some(bytes), Some("complete")),
        (None, Some(bytes), _) => (Some(bytes), Some("staging")),
        (None, None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None, None) => (None, None),
    };
    Par2GateOutcome {
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
        volume_file_seen,
        admitted,
        authoritative_verify_calls: pipeline.par2_authoritative_verify_calls,
        verdict_reached: pipeline.par2_authoritative_verify_calls > 0
            || pipeline.direct_session_pass_calls > 0
            || !pipeline.direct_verify_read_splits.is_empty(),
        demotions: sets,
    }
}

#[tokio::test]
async fn an_obfuscated_rar4_set_admits_from_par2_identity() {
    // The fingerprint rung is format-blind: the descriptions carry the real
    // RAR4 names, name classification orders them, and the per-volume
    // fingerprint places each file — none of which needs a volume number in
    // the headers. This is exactly the set shape header-based ordering can
    // never place (RAR4 interior volumes are indistinguishable), so the
    // PAR2-identity route is the only streaming admission it will ever have.
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 189) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41095),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        direct.admitted,
        "an obfuscated RAR4 set must admit from PAR2 identity, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "an identity-admitted RAR4 job must never create a source volume file, got {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the RAR4 set should have finalized, got {}",
        direct.demotions
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the RAR4 identity job must reproduce the member payload; status {:?}",
        direct.status
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?}",
        direct.status
    );
}

#[tokio::test]
async fn an_identity_binding_contradicted_by_the_volumes_own_number_demotes() {
    // The hardening: the fingerprint placed this file at volume 1, but the
    // volume's own main header declares position 2. One of the two is
    // describing a different archive, so the set demotes before the layout
    // adopts a member from the wrong position — and the fingerprints still
    // match, because they hash the bytes as posted, tampered header and all.
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 187) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 3);
    {
        let chunk = payload.len().div_ceil(3);
        let part = &payload[chunk..2 * chunk];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&TEST_RAR5_SIG);
        // Volume 1's head, rebuilt to lie: it claims position 2.
        bytes.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
        bytes.extend_from_slice(&build_test_rar_file_header(
            member_name,
            0x0008 | 0x0010,
            part.len() as u64,
            payload.len() as u64,
            Some(checksum::crc32(part)),
        ));
        bytes.extend_from_slice(part);
        bytes.extend_from_slice(&build_test_rar_end_header(true));
        volumes[1].1 = bytes;
    }
    // Inline rather than through the gate harness: the demotion hands the
    // job to the conventional path, which can complete and prune the
    // direct-store runtime before an end-of-run snapshot would look, so the
    // verdict has to be read mid-run — right after the lying volume's first
    // article.
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41096);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;

    submit_volume_article(&mut pipeline, job_id, &obfuscated, 0, 0).await;
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the set should admit on the honest first volume"
    );
    submit_volume_article(&mut pipeline, job_id, &obfuscated, 1, 0).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(IdentityVolumeMismatch)"),
        "the declared number must contradict the binding and demote, got {sets}"
    );
    let _ = working_dir;
}

#[tokio::test]
async fn a_par2_less_obfuscated_rar5_set_admits_from_its_own_headers() {
    // The header rung: no PAR2 anywhere, hex names carrying nothing — the
    // only structure left is what each volume's own RAR5 main header states,
    // and that is a position. The set opens on the first volume, grows a
    // binding per file, closes when the final volume's end record declares
    // itself last, and finalizes like any other set.
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let obfuscated = obfuscate_volumes(&volumes);
    // The very first decoded article is a payload article, not an
    // offset-zero one — the production arrival shape — so the admitting
    // binding itself runs the reorder-stage reclaim.
    let arrivals = [(0, 1), (0, 0), (1, 0), (1, 1), (2, 0), (2, 1)];

    let outcome =
        run_obfuscated_headers_gate(JobId(41097), member_name, &obfuscated, &arrivals).await;

    assert!(
        outcome.admitted,
        "a par2-less obfuscated RAR5 set must admit from its own headers, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "a header-admitted set must never create a source volume file, got {}",
        outcome.demotions
    );
    assert!(
        outcome.demotions.contains("Finalized"),
        "the header set should have closed and finalized, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the header-admitted set must reproduce the member payload; status {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_par2_less_obfuscated_standalone_rar5_admits_as_a_set_of_one() {
    // A single archive with no volume flag: a set of one, closed at
    // admission. The common single-rar obfuscated post.
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..60_000u32).map(|index| (index % 177) as u8).collect();
    let member_crc = checksum::crc32(&payload);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEST_RAR5_SIG);
    bytes.extend_from_slice(&build_test_rar_main_header(0, None));
    bytes.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0,
        payload.len() as u64,
        payload.len() as u64,
        Some(member_crc),
    ));
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&build_test_rar_end_header(false));
    let obfuscated = vec![(format!("{:032x}", 0xd1c7_0100_u128), bytes)];

    let outcome =
        run_obfuscated_headers_gate(JobId(41098), member_name, &obfuscated, &[(0, 0), (0, 1)])
            .await;

    assert!(
        outcome.admitted,
        "a standalone obfuscated RAR5 must admit from its own head, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "the standalone archive must never materialize, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the standalone archive must extract in-stream; status {:?}, sets {}",
        outcome.status,
        outcome.demotions
    );
}

#[tokio::test]
async fn a_par2_less_obfuscated_rar4_set_stays_conventional() {
    // The measured boundary, kept deliberately: RAR4 headers carry no volume
    // number and the interior volumes of a stored set are identical in every
    // field that could place one, so with no descriptions to consult there
    // is nothing to bind on. The header rung declines rather than guesses.
    let member_name = "Silver.Horizon.S01E16.mkv";
    let payload: Vec<u8> = (0..60_000u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41099);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &obfuscated);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(obfuscated.len()) {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }

    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "obfuscated RAR4 without descriptions must not admit"
    );
    assert!(
        obfuscated
            .iter()
            .all(|(filename, _)| working_dir.join(filename).exists()),
        "every volume must have streamed conventionally to disk"
    );
}

#[tokio::test]
async fn interleaved_obfuscated_header_sets_are_refused_not_guessed() {
    // Two obfuscated RAR5 sets in one job both open with a volume claiming
    // position zero. The bytes carry positions but no set identity, so the
    // second claimant is indistinguishable interleaving: the one header set
    // demotes, the rung is poisoned against forming another, and everything
    // streams conventionally.
    let first = single_member_store_set(
        "Silver.Horizon.S01E18.mkv",
        &(0..60_000u32)
            .map(|index| (index % 171) as u8)
            .collect::<Vec<u8>>(),
        2,
    );
    let second = single_member_store_set(
        "Amber.Sky.S01E18.mkv",
        &(0..60_000u32)
            .map(|index| (index % 167) as u8)
            .collect::<Vec<u8>>(),
        2,
    );
    let mut volumes = first.clone();
    volumes.extend(second);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41100);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &obfuscated);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // First set's volume 0 admits; the second set's volume 0 (file index 2)
    // claims the same position and condemns the rung.
    for (file_index, segment_number) in [
        (0, 0),
        (2, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (2, 1),
        (3, 0),
        (3, 1),
    ] {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(IdentityRosterUnfillable)"),
        "the ambiguous header set must demote, got {sets}"
    );
    assert_eq!(
        pipeline.direct_store.sets_for(job_id).len(),
        1,
        "poisoning must prevent a second header set from forming, got {sets}"
    );
    assert!(
        obfuscated
            .iter()
            .all(|(filename, _)| working_dir.join(filename).exists()),
        "every volume must end up conventional after the refusal"
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
    /// demotion, sampled before anything else drives the job. Without it the
    /// job's next move waits on the 30 s reconcile sweep.
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

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
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
            .or_else(|| staging_member(&complete_dir, member_name))
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
    // descriptions and slice checksums but no recovery blocks — the test
    // harness has no builder for a parseable multi-file recovery stream — so
    // neither gate can repair, and the two reach that dead end through
    // different waits (the direct gate through its demotion, the conventional
    // one through the targeted-recovery wait). What verification owns is the
    // verdict and the bytes, and both are asserted above; repair-to-success is
    // the repair differential.
}

// ---------------------------------------------------------------------------
// Destinations, finalization and demotion accounting
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
    let member_path = crate::pipeline::Pipeline::member_output_paths(
        &payload_root(&temp_dir, JobId(41014)),
        member_name,
    )
    .0;
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
        !direct_partial(&temp_dir, JobId(41014), member_name).exists(),
        "a duplicate after finalization must not recreate the partial"
    );
}

/// Removes one segment from the job's queue, standing in for the dispatch that
/// pops it in the real pipeline. Without this the harness's queue still holds
/// every article, and "already queued" would cover everything.
fn take_queued_segment(pipeline: &mut Pipeline, job_id: JobId, segment_id: SegmentId) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let queued = state.download_queue.drain_all();
    let before = queued.len();
    let kept: Vec<_> = queued
        .into_iter()
        .filter(|work| work.segment_id != segment_id)
        .collect();
    let removed = before - kept.len();
    for work in kept {
        state.download_queue.push(work);
    }
    assert!(
        removed > 0,
        "the segment must have been queued before it is dispatched"
    );
}

async fn route_articles_as_dispatched(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    articles: usize,
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", volumes, articles);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..articles as u32 {
            take_queued_segment(
                &mut pipeline,
                job_id,
                SegmentId {
                    file_id: NzbFileId { job_id, file_index },
                    segment_number,
                },
            );
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                volumes,
                file_index,
                segment_number,
                articles,
            )
            .await;
        }
    }

    (pipeline, working_dir)
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
    before_demotion: impl FnOnce(&mut Pipeline, &std::path::Path),
) -> (Pipeline, std::path::PathBuf, u64) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
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

    before_demotion(&mut pipeline, &working_dir);
    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded)
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

    // Volume 0 was covered end to end, so it is rebuilt byte-exactly from its
    // envelope plus the member partial — the whole point of reconstruction is
    // that those two articles never touch the network again.
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
    assert!(!direct_partial(&temp_dir, JobId(41015), member_name).exists());
    for volume_index in 0..volumes.len() as u32 {
        assert!(
            !working_dir
                .join(format!("silver.horizon.vol{volume_index:05}.envelope"))
                .exists(),
            "envelope {volume_index} must be deleted once the volume is real"
        );
    }

    // Reconciliation persisted legacy state in that order and shape: a whole
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

/// A demoted set's volumes must re-enter the conventional completion seam.
///
/// While the set was direct, `refresh_archive_state_for_completed_file`
/// suppressed itself for its files, so none of them ever reached the RAR
/// facts parser or the archive topology — correctly, because a direct set
/// never extracts through the topology. Demotion ends that: the volumes are
/// ordinary files now, and a materialized-complete volume that never gets
/// its facts registered is invisible to the extraction planner forever —
/// the plan waits on a volume whose bytes sit finished on disk, and no
/// event ever arrives to change its mind.
#[tokio::test]
async fn a_demoted_sets_materialized_volumes_register_their_rar_facts() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41033);
    let (pipeline, _working_dir, _) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _| {}).await;

    let facts_registered = pipeline
        .rar_sets
        .iter()
        .any(|((facts_job_id, _), state)| *facts_job_id == job_id && state.facts.contains_key(&0));
    assert!(
        facts_registered,
        "the materialized first volume must have parsed RAR facts through the \
         conventional completion replay; rar_sets = {:?}",
        pipeline
            .rar_sets
            .iter()
            .map(|((jid, name), state)| (
                jid.0,
                name.clone(),
                state.facts.keys().collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_malformed_chain_demotion_leaves_a_partial_crc_atom_provisional() {
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41032);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |pipeline, _| {
            // The SQLite ordering from the malformed-chain fixture: durable
            // placement reaches into the next article before the chain
            // contradiction demotes the set. Its whole-article CRC exists, but
            // the placed prefix cannot be composed against it exactly.
            let (start, end) = article_extent(volumes[1].1.len(), 1, 2);
            let partial_len = (end - start).div_ceil(2);
            let set = pipeline.direct_store.set_mut(job_id, 0).unwrap();
            set.note_volume_part_crc(
                1,
                start as u64,
                (end - start) as u64,
                par2_rs::checksum::crc32(&volumes[1].1[start..end]),
            );
            set.record_writes(
                &[crate::pipeline::direct_store::router::RoutedSpan {
                    destination:
                        crate::pipeline::direct_store::router::DirectDestination::Envelope {
                            volume_index: 1,
                        },
                    destination_offset: start as u64,
                    volume_index: 1,
                    source_offset: start as u64,
                    bytes: vec![0xA5; partial_len],
                }],
                std::time::Instant::now(),
            );
            assert_eq!(
                set.volume_coverage(1).end(),
                (start + partial_len) as u64,
                "the rig must end physical coverage inside the CRC atom"
            );
            set.demote(DemotionReason::MemberIneligible(
                crate::pipeline::direct_store::router::MemberIneligibility::MalformedChain,
            ));
        })
        .await;

    let volume_one_prefix = volumes[1].1.len().div_ceil(2);
    let materialized_volume_one = std::fs::read(working_dir.join(&volumes[1].0)).unwrap();
    let provisional_len = (volumes[1].1.len() - volume_one_prefix).div_ceil(2);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the complete neighbour must survive the malformed tail"
    );
    assert_eq!(
        materialized_volume_one.len(),
        volume_one_prefix + provisional_len,
        "physical geometry is retained even where proof coverage stops"
    );
    assert_eq!(
        &materialized_volume_one[..volume_one_prefix],
        &volumes[1].1[..volume_one_prefix],
        "the wholly CRC-vouched article must materialize byte for byte"
    );
    assert!(
        materialized_volume_one[volume_one_prefix..]
            .iter()
            .all(|byte| *byte == 0),
        "the partial CRC atom must remain a sparse, unowned hole"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(1, 1), (2, 0), (2, 1)],
        "the provisional article is targeted for conventional ownership without refetching its complete neighbours"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volumes[0].1.len() as u64 + volume_one_prefix as u64,
        "only materialized article extents remain counted"
    );
    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(job_id)).contains("MalformedChain"),
        "the regression must retain the malformed-chain demotion reason"
    );
}

#[tokio::test]
async fn a_demotion_falls_back_to_refetching_when_its_envelope_is_gone() {
    // Reconstruction is an optimisation; every one of its failure modes has to
    // land on the always-correct refetch rather than on a half-written volume.
    // Deleting the envelope out from under the sweep is the bluntest of them:
    // the header bytes it needs are simply not there any more.
    let member_name = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41031);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
            assert!(envelope.exists(), "the envelope must exist to be deleted");
            std::fs::remove_file(&envelope).unwrap();
        })
        .await;

    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "a failed reconstruction must not leave a partly written volume behind"
    );
    let expected = vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)];
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        expected,
        "the fallback refetches every article exactly once"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        volumes.len(),
        "every reset source volume remains behind the PAR2 gate"
    );

    let set_id = par2_rs::RecoverySetId::from_bytes([41; 16]);
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "quiescent missing articles must be rescued before PAR2 can observe them"
    );
    assert_eq!(
        pipeline
            .direct_store
            .rescued_materialization_segments(job_id),
        expected.len(),
        "each ownerless segment receives one ordinary retry lineage"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "the queued rescue remains an owner and must not be duplicated"
    );
    assert_eq!(
        pipeline
            .direct_store
            .rescued_materialization_segments(job_id),
        expected.len()
    );
    let rescued = queued_segments(&mut pipeline, job_id);
    assert_eq!(rescued, expected);
    pipeline
        .terminal_segment_failures
        .extend(
            rescued
                .iter()
                .map(|(file_index, segment_number)| SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: *file_index,
                    },
                    segment_number: *segment_number,
                }),
        );
    assert!(
        pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "terminally unavailable articles release the demotion gate"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0
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

/// Paging, at the seam the RAM budget used to demote at.
///
/// The first article is pure payload with no header yet, so it has nowhere to
/// go and is held; the budget is far below it. The router pages it to the set's
/// scratch file instead of demoting, and the set stays live.
#[tokio::test]
async fn direct_store_pages_held_bytes_to_scratch_instead_of_demoting() {
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
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

/// A ceiling breach is not the same thing as a full scratch.
///
/// The scratch is an append-only log, so a hold that gets placed leaves its
/// region behind: a set that pages, places, and pages again walks the cursor up
/// to the ceiling while holding almost nothing. Reclaiming that space is what
/// keeps such a set from demoting — and demotion here is expensive, because it
/// materializes the volumes and can refetch them.
#[tokio::test]
async fn a_reclaimable_scratch_breach_spills_instead_of_demoting() {
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Each volume's payload before its own header, so every volume pages its
    // hold and then has it placed before the next one arrives.
    let arrivals = [(0u32, 1u32), (0, 0), (1, 1), (1, 0), (2, 1), (2, 0)];

    // Room for one volume's held payload but not two in sequence: the second
    // page only fits once the first volume's placed region is reclaimed.
    let direct = run_direct_store_gate_with_ceilings(
        DirectStoreGate::Enabled,
        Some(64),
        Some(600),
        JobId(41055),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate_with_budget(
        DirectStoreGate::Disabled,
        None,
        JobId(41056),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "the set must have stayed direct: a demotion materializes the volumes"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "and a set that reclaimed its scratch must still match the conventional extractor \
         exactly"
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
    let job_id = JobId(41054);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchCeiling)"),
        "a scratch ceiling breach must demote with its own reason, got {shape}"
    );
    assert!(!direct_partial(&temp_dir, JobId(41054), member_name).exists());
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
// Chain-close eligibility
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
        !direct_partial(&temp_dir, JobId(41021), member_name).exists(),
        "the demotion deletes the bytes routed while the member was provisional"
    );

    // The label is not the outcome. The demotion runs reconstruction, and the
    // member whose eligibility just flipped is the one holding most of every
    // volume's bytes: a sweep that asked the *current* classification where
    // they live would be told "the envelope", read a sparse hole, and write
    // zeros into a volume file under a published floor. Whatever the demotion
    // decides to do, what lands on disk has to be the volume.
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
// The yEnc whole-volume gate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_demotes_a_volume_whose_yenc_whole_file_crc_disagrees() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41022);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

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
    assert!(!direct_partial(&temp_dir, JobId(41022), member_name).exists());
}

// ---------------------------------------------------------------------------
// Suppression and runtime lifetime
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_complete_direct_volume_never_refreshes_archive_state() {
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
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
// Format detection
// ---------------------------------------------------------------------------

/// The RAR4 twin of [`single_member_store_set`].
///
/// RAR4 states the whole-member CRC32 in the *last* part's header and each
/// earlier part's own packed CRC32 in its own — the same two integrity layers,
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
        std::fs::read(
            crate::pipeline::Pipeline::member_output_paths(
                &payload_root(&temp_dir, JobId(41025)),
                member_name
            )
            .0
        )
        .ok()
        .as_deref(),
        Some(payload.as_slice()),
        "the routed RAR4 member must reproduce the payload byte for byte"
    );
}

// ---------------------------------------------------------------------------
// Multi-member sets
// ---------------------------------------------------------------------------

/// A store set carrying several members, split across `volume_count` volumes.
///
/// The members are laid end to end and the concatenation is cut into equal
/// volume payloads, so member boundaries and volume boundaries deliberately do
/// **not** line up: members start mid-volume, at least one is split across
/// volumes, and one volume carries the tail of one member and the head of the
/// next. That is the shape a season pack posts as, and the shape the first
/// shape demoted on sight.
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
                // The RAR5 rule the per-part layer reads: a non-final part
                // states the CRC32 of *its own* packed bytes, the final part
                // the whole member's.
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
            let staged = staging_member(&complete_dir, name);
            let left_behind = std::fs::read(working_dir.join(name)).ok();
            assert!(
                completed.is_none() || (staged.is_none() && left_behind.is_none()),
                "{name} must exist in exactly one place"
            );
            match (completed, staged, left_behind) {
                (Some(bytes), _, _) => (name.to_string(), Some(bytes), Some("complete")),
                (None, Some(bytes), _) => (name.to_string(), Some(bytes), Some("staging")),
                (None, None, Some(bytes)) => (name.to_string(), Some(bytes), Some("working")),
                (None, None, None) => (name.to_string(), None, None),
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
// Envelope v2: recovery records route
// ---------------------------------------------------------------------------

/// A store set whose volumes each carry a recovery record after the payload.
///
/// The RR is a service header plus a data area belonging to no member, so every
/// byte of it is envelope. At `rr_bytes` well over the old 32 KiB half-slot
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
    // slot the first shape gave a volume, let alone the 32 KiB half it
    // addressed the head from. This set could not have routed a byte before
    // envelope v2.
    const { assert!(RR_BYTES > 32 * 1024) };

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
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
            let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
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
        std::fs::read(
            crate::pipeline::Pipeline::member_output_paths(
                &payload_root(&temp_dir, JobId(41045)),
                member_name
            )
            .0
        )
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
// The hybrid virtual-volume provider, differentially
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
// The classification frontier
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
            std::fs::read(
                crate::pipeline::Pipeline::member_output_paths(
                    &payload_root(&temp_dir, JobId(41047)),
                    name
                )
                .0
            )
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
// Demotion after a routed member turns ineligible
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
    for (volume_index, (filename, bytes)) in volumes.iter().enumerate() {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "volume {volume_index} must come back byte for byte, whether reconstructed from \
             routed extents or completed by the conventional handoff"
        );
    }
    assert!(
        !direct_partial(&temp_dir, JobId(41051), member_name).exists(),
        "the direct outputs are deleted once the volumes are real"
    );
}

#[tokio::test]
async fn a_partially_covered_volume_is_verified_before_it_is_materialized() {
    // Per-run composition plus the no-reference refusal: a volume covered only
    // as far as its first article still has a composed reference for that
    // prefix, and the sweep checks it. Corrupting the member partial under the
    // covered prefix is what a partial that came back wrong looks like — and it
    // has to end in the refetch, not in a volume file nothing verified.
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
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _working_dir| {
            use std::io::{Seek, SeekFrom, Write};
            let partial = direct_partial(&temp_dir, JobId(41052), member_name);
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
// The staged-bytes budget
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
    let envelope = std::fs::read(working_dir.join("silver.horizon.f0.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "the recovery record was routed into the envelope, so it is not a hold"
    );

    // Part three: the same sequence under a budget smaller than the retained
    // region pages it out. Counting only unrouted holds — as the first shape
    // did — would have found nothing to count at all, so nothing would have
    // breached and nothing would have paged; the retained recovery record is
    // what proves the accounting reaches the term RSS actually pays for.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(BUDGET);
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
    let envelope = std::fs::read(paged_dir.join("silver.horizon.f0.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "paging the retained copy must not disturb what was already written"
    );
}

// ---------------------------------------------------------------------------
// Zero-length stored members
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
    // Nothing is ever routed for a zero-length member, so the byte-driven
    // whole-member gate can never fire for it. The first shape left it
    // unverified for the life of the job: the set never finalized, never
    // demoted, and kept its suppressions armed over files that would never
    // exist. It verifies trivially instead — CRC32 of no bytes is zero, which
    // is what the header states — and finalization creates the file.
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
// The bounded small-member tolerance
// ---------------------------------------------------------------------------

/// What the extra, ineligible member of a tolerance fixture looks like.
#[derive(Clone, Copy)]
enum ToleranceExtra {
    /// An **unsplit** stored member whose header carries a real BLAKE2sp digest
    /// and no CRC32.
    ///
    /// Unsplit is load-bearing: the classifier only reaches the hash fields
    /// once the chain is complete, so a *split* BLAKE2sp-only member is
    /// `ProvisionallyDirect` — and routes into a partial — until its last
    /// header lands. An unsplit one is `Ineligible` from its single header, so
    /// every byte of it goes to the envelope, which is the shape the
    /// small-member tolerance describes and the one the extraction can read
    /// back.
    Blake2OnlyStore,
    /// A stored member with a real BLAKE2sp digest and no CRC32, **split**
    /// across the last two volumes.
    ///
    /// The case the unsplit variant above cannot reach: the classifier only sees
    /// the hash fields when the chain completes, so this member is
    /// `ProvisionallyDirect` from its first header, gets adopted, and routes its
    /// first part into a `.direct.partial` — and only then, at chain close,
    /// resolves `Blake2OnlyNoCrc32`. Its already-routed bytes are in the wrong
    /// file for the tolerance, which is what the migration exists to fix.
    Blake2OnlySplit,
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
    // The split shapes put their first half one volume earlier.
    let split_extra = matches!(
        extra,
        ToleranceExtra::CompressedSplit | ToleranceExtra::Blake2OnlySplit
    );
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
                let header =
                    match extra {
                        ToleranceExtra::Blake2OnlyStore => build_test_rar_file_header_with_extra(
                            extra_name,
                            extra_flags,
                            extra_part.len() as u64,
                            extra_payload.len() as u64,
                            None,
                            &build_test_rar_blake2_extra(unrar_rs::crypto::blake2sp_hash(
                                extra_payload,
                            )),
                        ),
                        // What `rar -m0 -htb` writes for a member split across
                        // volumes: **no** CRC32 anywhere, a BLAKE2sp packed hash per
                        // non-final part and the whole-member BLAKE2sp on the last.
                        // The open chain is what makes it `ProvisionallyDirect` —
                        // routable, and routed — and the final header is what
                        // resolves it `Blake2OnlyNoCrc32`.
                        ToleranceExtra::Blake2OnlySplit => build_test_rar_file_header_with_extra(
                            extra_name,
                            extra_flags,
                            extra_part.len() as u64,
                            extra_payload.len() as u64,
                            None,
                            &build_test_rar_blake2_extra(unrar_rs::crypto::blake2sp_hash(
                                if is_last { extra_payload } else { extra_part },
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

/// The case an earlier shape could only demote: a BLAKE2sp-only member the
/// router adopted while its chain was open, whose routed bytes are migrated out
/// of its partial and into the envelope so it can ride the tolerance after all.
#[tokio::test]
async fn a_split_blake2_only_member_migrates_to_the_envelope_and_matches_the_extractor() {
    let store_name = "Silver.Horizon.S01E51.mkv";
    let extra_name = "Silver.Horizon.S01E51.nfo";
    // At least 100x the extra, so the extra fits under
    // `min(64 MiB, 1% of packed archive bytes)`.
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41091),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41092),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    // Non-vacuity, and the whole point: an earlier shape demoted this set, and
    // a demotion materializes every volume. A run with no volume on disk is a
    // run that stayed direct through the chain close.
    assert!(
        !direct.volume_file_seen,
        "the migration must keep the set direct: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the BLAKE2sp-only member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the migrated member is extracted from the virtual volumes and the stored one \
         from its own partial; both must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_split_blake2_only_member_over_the_budget_demotes_on_its_own_reason() {
    let store_name = "Silver.Horizon.S01E52.mkv";
    let extra_name = "Silver.Horizon.S01E52.nfo";
    // Ten percent of the archive, not one: nothing this size may be moved into
    // the envelope, however cheap the copy would be.
    let store_payload: Vec<u8> = (0..2_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let shape = tolerance_shape(JobId(41093), &volumes).await;
    // Its **own** reason, not the tolerance's: the member is over the budget,
    // but what ended its routing is that direct-store cannot verify it, and that
    // is the population the metric has always counted here.
    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "an adopted member too large to migrate must demote exactly as it did before \
         the migration existed, got {shape}"
    );
}

#[tokio::test]
async fn a_migration_that_cannot_read_its_partial_demotes_cleanly() {
    let store_name = "Silver.Horizon.S01E53.mkv";
    let extra_name = "Silver.Horizon.S01E53.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41094);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Everything up to the volume that closes the chain. The extra member's
    // first part is routed into its partial by the volume before it.
    let last = volumes.len() as u32 - 1;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index == last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let partial = direct_partial(&temp_dir, JobId(41094), extra_name);
    assert!(
        std::fs::metadata(&partial).is_ok_and(|metadata| metadata.len() > 0),
        "non-vacuity: the member must really have been adopted and routed before the \
         migration is asked to move its bytes"
    );
    // The migration's read is what fails: the bytes it is told are there are
    // gone. Truncation rather than deletion, so the destination the checkpoint
    // claims still exists and only the read can fail.
    std::fs::File::create(&partial).unwrap();

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index != last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "a migration that cannot read the bytes it is moving must demote exactly as a \
         set with no migration at all would have, got {shape}"
    );
    // Cleanly: nothing half-moved, and nothing fabricated. The migration mutates
    // no state until every byte is in hand, so the routing history still claims
    // the member's extents and the demotion sweep reads them from the partial —
    // which is now empty, so it refuses rather than writing zeros into a volume.
    assert_volumes_are_never_fabricated(&working_dir, &volumes);
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
// Review fixes: what a *later* PAR2 pass sees, what an unbindable volume does,
// and what a mid-download set is protected from.
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
    // The fixture submits articles directly rather than dequeuing them.  Model
    // the exhausted discovery state before a later completion pass.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    (pipeline, working_dir)
}

fn no_volume_file(working_dir: &std::path::Path, volumes: &[(String, Vec<u8>)]) -> bool {
    volumes
        .iter()
        .all(|(filename, _)| !working_dir.join(filename).exists())
}

#[tokio::test]
async fn a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass() {
    // Finalization is what makes a direct set's source volumes permanently
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
    // The overlay is keyed by PAR2 file id, so a volume whose identity does not
    // resolve to exactly one description can neither be served virtually nor be
    // blamed for the damage the pass then reports about it. Before this fix the
    // set stayed direct and the repairer was handed a virtual volume.
    let member_name = "Silver.Horizon.S01E22.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41052);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

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
    // same recovery set and `resolve_par2_file_binding` refuses to pick one.
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
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0
            })
            .is_none(),
        "non-vacuity: volume 0's name candidates must match two descriptions, so no \
         single PAR2 identity can be chosen for it"
    );
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
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
        .or_else(|| staging_member(&complete_dir, member_name))
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
    // A set that is still receiving articles has holes where the rest of its
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
    let verification = par2_rs::VerificationResult {
        files: overlay
            .volumes
            .iter()
            .map(|volume| par2_rs::verify::FileVerification {
                file_id: volume.par2_file_id,
                filename: format!("virtual volume {}", volume.volume_index),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false; 4],
                missing_slice_count: 4,
            })
            .collect(),
        recovery_blocks_available: 0,
        total_missing_blocks: 4 * overlay.volumes.len() as u32,
        repairable: par2_rs::verify::Repairability::NotNeeded,
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
        pipeline.par2_authoritative_verify_calls > verifies_before || settled.contains("Finalized"),
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
    // The tolerated extraction reads the *virtual volumes*: the envelopes
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
// Restart
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

/// [`direct_store_before_restart`] with one extra input. The password is
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
/// `complete_files` and `file_progress` are deliberately **empty**. Suppression
/// keeps both of them empty for a direct set's source volumes — no legacy
/// floor, no completed-file row — so a restore that skips anything at all is
/// skipping it on the strength of the direct checkpoint and nothing else.
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

/// Reads the member out of wherever the gate left it, the same three candidate
/// places every other differential here checks.
///
/// `staging` is the middle one and it is not hypothetical: both direct
/// finalization and the incremental extractor write a member into the job's
/// staging root, and it only reaches `complete` when the final move renames it
/// out. A gate that stopped before the move leaves it there.
fn member_after_gate(
    complete_dir: &Path,
    working_dir: &Path,
    member_name: &str,
) -> (Option<Vec<u8>>, Option<&'static str>) {
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    match (
        std::fs::read(output_root.join(member_name)).ok(),
        staging_member(complete_dir, member_name),
        std::fs::read(working_dir.join(member_name)).ok(),
    ) {
        (Some(bytes), _, _) => (Some(bytes), Some("complete")),
        (None, Some(bytes), _) => (Some(bytes), Some("staging")),
        (None, None, Some(bytes)) => (Some(bytes), Some("working")),
        (None, None, None) => (None, None),
    }
}

#[tokio::test]
async fn restart_after_refetch_demotion_restores_incomplete_source_ownership() {
    let member_name = "Silver.Horizon.S01E25.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41057);

    let (pipeline, working_dir, _) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            std::fs::remove_file(working_dir.join("silver.horizon.f0.vol00000.envelope")).unwrap();
        })
        .await;
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "the direct checkpoint must be retired before the simulated crash"
    );
    let (persisted_progress, persisted_complete) =
        pipeline.db.load_active_file_runtime(job_id).unwrap();
    assert!(persisted_progress.is_empty() && persisted_complete.is_empty());
    let file_progress = persisted_progress;
    let complete_files = persisted_complete
        .into_iter()
        .map(|file_index| NzbFileId { job_id, file_index })
        .collect();
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        volumes.len(),
        "the live process still owns the demotion gate before the crash"
    );
    drop(pipeline);

    let (mut restarted, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    restarted.direct_store.set_gate(DirectStoreGate::Enabled);
    restarted
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: direct_store_job_spec("Silver Horizon", &volumes),
            complete_files,
            file_progress,
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

    assert_eq!(
        restarted.direct_store.pending_materialization_files(job_id),
        0,
        "the in-memory gate is intentionally not persisted"
    );
    let expected: Vec<(u32, u32)> = (0..volumes.len() as u32)
        .flat_map(|file_index| [(file_index, 0), (file_index, 1)])
        .collect();
    assert_eq!(peek_queued_segments(&mut restarted, job_id), expected);
    assert!(
        restarted.job_has_pending_download_pipeline_work(job_id),
        "ordinary restart assembly must hold PAR2 behind the queued source articles"
    );

    for (file_index, segment_number) in expected {
        dispatch_and_submit(
            &mut restarted,
            job_id,
            &volumes,
            file_index,
            segment_number,
            2,
        )
        .await;
    }
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "a checkpoint-free restart may re-admit the incomplete set, but it must \
         retain the no-source-volume direct path"
    );
    assert!(
        format!("{:?}", restarted.direct_store.sets_for(job_id)).contains("Finalized"),
        "the restarted set must not settle until all source articles have been rerouted"
    );

    restarted.schedule_job_completion_check(job_id);
    drain_rar_refreshes(&mut restarted).await;
    drive_extractions_to_terminal(&mut restarted, job_id, 64).await;
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(member.as_deref(), Some(payload.as_slice()));
    assert!(matches!(
        job_status_for_assert(&restarted, job_id),
        Some(JobStatus::Complete)
    ));
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
    // Suppression re-armed: a restored set's source volumes are still direct,
    // so no legacy floor, completed-file row or archive re-probe may be written
    // for them. The restore itself relies on the same thing in the other
    // direction — no completed-file row exists for a direct volume by
    // construction, so the legacy skip plan contributes nothing and the
    // coverage row is the only reason anything is skipped at all.
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

/// A byte corrupted on disk while the process was down is caught by the
/// re-read, not committed.
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
    let partial = direct_partial(&temp_dir, JobId(41063), member_name);
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

/// The payload lands on the **complete** volume and the scratch does not.
///
/// The two are observed on a live job rather than derived from a plan, because
/// the derivation is only half the claim: what matters operationally is which
/// filesystem the bytes are actually written to as the articles arrive, and that
/// they are still there — under the staging root — when finalization renames
/// them into place.
#[tokio::test]
async fn a_direct_set_writes_payload_to_the_staging_root_and_scratch_to_the_working_dir() {
    let member_name = "Silver.Horizon.S01E44.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Small enough that the first held payload pages out, so the scratch file
    // this test is half about actually exists.
    pipeline.direct_store.set_holds_budget(64);
    let job_id = JobId(41120);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let staging = payload_root(&temp_dir, job_id);
    assert!(
        !staging.starts_with(&working_dir),
        "non-vacuity: the harness must give the job two genuinely separate roots"
    );

    // Payload before the header, so the router has to hold it.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    let scratch = working_dir.join(".weaver-holds.silver.horizon.f0");
    assert!(
        scratch.exists(),
        "the paged holds must reach a scratch file in the intermediate directory"
    );
    assert!(
        !staging.join(".weaver-holds.silver.horizon.f0").exists(),
        "the holds scratch is working data and must never be written to the complete volume"
    );

    // Mid-flight: the derivation the live set is actually routing through.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    {
        let set = pipeline
            .direct_store
            .set(job_id, 0)
            .expect("the set was admitted");
        let members = set.router.member_partials();
        assert!(!members.is_empty(), "non-vacuity: nothing routed");
        for (_, name, relative) in members {
            let partial = set.plan().destination_path(relative);
            let destination = set
                .plan()
                .member_output_path(name)
                .expect("the member resolves");
            assert!(
                partial.starts_with(&staging) && destination.starts_with(&staging),
                "{name}: both sides of the commit rename must be under the staging root"
            );
            assert!(
                !working_dir.join(relative).exists(),
                "{name}: no payload byte may be written into the intermediate directory"
            );
        }
        assert!(
            set.plan().holds_scratch_path().starts_with(&working_dir)
                && set.plan().envelope_path(0).starts_with(&working_dir),
            "the set's working files stay in the intermediate directory"
        );
    }
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "the envelopes are working data and belong beside the scratch"
    );
    assert_eq!(
        direct_envelopes_left(&staging),
        0,
        "and none of them may be written to the complete volume"
    );

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (0, 0) || (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must finalize, got {shape}"
    );

    assert_eq!(
        std::fs::read(staging.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "the committed member is in the staging root, ready to be published by rename"
    );
    assert!(
        !working_dir.join(member_name).exists(),
        "and nowhere in the intermediate directory"
    );
    assert!(
        !any_direct_partial(&working_dir),
        "no `.direct.partial` may ever have been created in the intermediate directory"
    );
}

/// A restart re-derives the same destinations, in the same staging root.
///
/// The staging root is deterministic per job id, so the "after" pipeline builds
/// it from the job id alone — before the job state exists — and has to arrive at
/// the byte-identical path the "before" pipeline wrote into. If it did not, every
/// destination claim in the checkpoint would fail its probe, the row would be
/// deleted and the set would redownload from zero: safe, silent, and a complete
/// loss of the resume.
#[tokio::test]
async fn a_restart_re_derives_its_destinations_in_the_same_staging_root() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E45.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41121);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let partial = direct_partial(&temp_dir, job_id, member_name);
    let before = std::fs::read(&partial).expect("volume 0 routed into the member partial");
    assert!(!before.is_empty(), "non-vacuity: nothing was routed");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The row survived, which is only possible if the probe found the partial —
    // and the probe joins the claim onto the root this pipeline re-derived.
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    assert!(
        set.has_restart_seeded_coverage(),
        "the checkpoint must have been accepted, not refused on a missing destination"
    );
    let staging = payload_root(&temp_dir, job_id);
    for (_, name, relative) in set.router.member_partials() {
        assert_eq!(
            set.plan().destination_path(relative),
            partial,
            "{name}: the resumed run must re-derive the very path the previous one wrote"
        );
        assert!(
            set.plan()
                .member_output_path(name)
                .expect("the member resolves")
                .starts_with(&staging)
        );
    }
    assert_eq!(
        std::fs::read(&partial).ok(),
        Some(before),
        "and the bytes under it are the ones the checkpoint claims"
    );

    // Non-vacuity for the *skip*: a restore that re-derived a different root
    // would refuse the row and requeue every segment of the set.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "the accepted checkpoint must let the resumed job skip what it already has, got {queued:?}"
    );

    // And it finishes: the resumed run's own writes land beside the restored
    // ones, in the same root, and the commit rename still never crosses.
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
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "a resumed set must finish from its checkpoint, got {shape}"
    );
    assert_eq!(
        std::fs::read(staging.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "and commit the member in the staging root, byte for byte"
    );
    assert!(
        !any_direct_partial(&working_dir) && !working_dir.join(member_name).exists(),
        "a resumed run leaves no payload in the intermediate directory either"
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

    let partial = direct_partial(&temp_dir, JobId(41064), member_name);
    let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
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

    let partial = direct_partial(&temp_dir, JobId(41065), member_name);
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

/// A member first seen in a **later volume** must not silently invalidate the
/// checkpoint the earlier volumes wrote.
///
/// The plan digest binds the member names and their unpacked sizes, so it
/// changes the moment a set adopts a member it had not seen — which is the
/// ordinary shape of a multi-member set, whose members are discovered in
/// whatever order their volumes arrive. A digest stamped once, at the first
/// member, describes a plan the restart no longer computes: every row written
/// afterwards is refused for a set nothing is wrong with, and the whole thing
/// redownloads.
#[tokio::test]
async fn a_member_first_seen_in_a_later_volume_still_restarts_from_its_checkpoint() {
    const ARTICLES: usize = 2;
    let episode = "Silver.Horizon.S01E40.mkv";
    let notes = "Silver.Horizon.S01E40.nfo";
    let members = vec![
        (
            episode,
            (0..2400u32).map(|index| (index % 251) as u8).collect(),
        ),
        // Sized so the split lands on a member boundary: the episode occupies
        // volumes 0 and 1 whole, and volume 2 holds nothing but the notes. The
        // notes' header therefore does not exist anywhere the barrier can see
        // until volume 2's first article arrives, long after the barrier was
        // built for the episode.
        (
            notes,
            (0..1200u32).map(|index| (index % 241) as u8).collect(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 3);
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41067);
    // Volume 0 whole, volume 1 half, and volume 2's header — the article that
    // introduces the second member.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0), (2, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Non-vacuity: the run really did discover the second member after the first
    // barrier existed, and really did route bytes for both.
    let episode_partial = direct_partial(&temp_dir, JobId(41067), episode);
    let notes_partial = direct_partial(&temp_dir, JobId(41067), notes);
    assert!(
        episode_partial.exists() && notes_partial.exists(),
        "both members must have routed before the restart"
    );

    // The committed blob, captured before the restore can delete it, so the
    // refusal path this test exists for can be named rather than inferred from a
    // redownload.
    let committed = {
        let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        rows.into_iter()
            .next()
            .expect("the shutdown barrier must have committed a row")
            .1
    };

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The invariant itself, stated where it can be read: the digest the row
    // carries is the digest this run computes for the same set.
    let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    let judgement = crate::pipeline::direct_store::restart::restore_set(
        &crate::pipeline::direct_store::restart::DestinationRoots::for_plan(set.plan()),
        &committed,
        &set.expected_set(),
    )
    .await;
    assert!(
        judgement.is_ok(),
        "the committed row describes this very set and must be accepted, got {:?}",
        judgement.err()
    );
    let blob = rows
        .get(set.set_name())
        .expect("an accepted row is kept until the next barrier replaces it");
    let snapshot = crate::pipeline::direct_store::snapshot::decode(blob)
        .expect("the committed row must decode");
    assert_eq!(
        snapshot.plan_digest,
        set.expected_set().plan_digest,
        "a checkpoint must be stamped with the digest of the plan it was written under, \
         including members the set adopted after the barrier was built"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "the row was accepted, so its coverage came back seeded and unverified"
    );
    assert!(
        episode_partial.exists() && notes_partial.exists(),
        "an accepted row keeps its destinations; only a refused set's files are swept"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, got {queued:?}"
    );
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
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
    let restarted: Vec<GateMember> = names
        .iter()
        .map(|name| {
            let (bytes, location) = member_after_gate(&complete_dir, &working_dir, name);
            (name.to_string(), bytes, location)
        })
        .collect();
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41068),
        &names,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
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
        (restarted, restarted_status),
        (conventional.members, conventional.status),
        "a restarted multi-member direct job must finish exactly as the conventional extractor"
    );
}

/// A restart in the window after a member migration keeps its checkpoint
/// (`task_9ee23560`).
///
/// The migration moves a tolerated split BLAKE2sp-only member's bytes into the
/// envelope and **unlinks its partial**. Both halves of this pass are needed for
/// the row to survive that: the barrier has to stop claiming the file that is
/// gone, and it has to re-stamp the plan digest the member's departure changed.
/// Either one missing is a refused row and a whole-set redownload.
#[tokio::test]
async fn a_restart_after_a_member_migration_keeps_its_checkpoint() {
    const ARTICLES: usize = 2;
    let store_name = "Silver.Horizon.S01E54.mkv";
    let extra_name = "Silver.Horizon.S01E54.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41069);
    // Every article but volume 1's second. The extra's chain closes on the last
    // volume, so the migration has run; the hole in volume 1 is what keeps the
    // set mid-download, which is the whole window this test is about.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0), (2, 0), (2, 1), (3, 0), (3, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Non-vacuity: the migration really ran, and it really did leave the routed
    // member's own destination alone.
    let extra_partial = direct_partial(&temp_dir, JobId(41069), extra_name);
    let store_partial = direct_partial(&temp_dir, JobId(41069), store_name);
    assert!(
        !extra_partial.exists(),
        "the migration must have deleted the migrated member's partial"
    );
    assert!(
        store_partial.exists(),
        "the routed member's partial must be untouched by the migration"
    );

    let committed = {
        let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        rows.into_iter()
            .next()
            .expect("the shutdown barrier must have committed a row")
            .1
    };
    let snapshot = crate::pipeline::direct_store::snapshot::decode(&committed)
        .expect("the committed row must decode");
    let claimed: Vec<&str> = snapshot
        .destinations
        .iter()
        .map(|claim| claim.relative_path.as_str())
        .collect();
    assert!(
        !claimed.contains(&format!("{extra_name}.f0.direct.partial").as_str()),
        "the checkpoint may not claim a destination the migration deleted, got {claimed:?}"
    );
    assert!(
        claimed.contains(&format!("{store_name}.f0.direct.partial").as_str()),
        "non-vacuity: the surviving member is still claimed, got {claimed:?}"
    );

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    let judgement = crate::pipeline::direct_store::restart::restore_set(
        &crate::pipeline::direct_store::restart::DestinationRoots::for_plan(set.plan()),
        &committed,
        &set.expected_set(),
    )
    .await;
    assert!(
        judgement.is_ok(),
        "a checkpoint written after a migration describes the set that comes back and must \
         be accepted, got {:?}",
        judgement.err()
    );
    assert!(
        store_partial.exists(),
        "an accepted row keeps its destinations; only a refused set's files are swept"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.iter().all(|(file_index, _)| *file_index == 1),
        "every volume the checkpoint calls complete must stay off the queue, got {queued:?}"
    );
    // Volume 1 is the partially covered one, and it pays the documented
    // one-article rounding: the floor counts decoded bytes while the spec
    // declares yEnc-encoded ones, so the last checkpointed article of a
    // part-covered volume comes back with the one that never arrived.
    assert!(
        queued.contains(&(1, 1)),
        "the article that never arrived must be refetched, got {queued:?}"
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
    let restarted: Vec<GateMember> = [store_name, extra_name]
        .iter()
        .map(|name| {
            let (bytes, location) = member_after_gate(&complete_dir, &working_dir, name);
            (name.to_string(), bytes, location)
        })
        .collect();
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41070),
        &[store_name, extra_name],
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the migrated member"
    );
    assert_eq!(
        (restarted, restarted_status),
        (conventional.members, conventional.status),
        "a set restarted after a migration must extract both members byte-identically to \
         the conventional extractor"
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

/// A restart inside the PAR2 finalization wait — the common case now, because a
/// par2-bearing set stays byte-complete-but-uncommitted for the whole PAR2
/// download and verify.
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
    // topology**, which a direct set never enters by construction. While the
    // job is live that never showed, because live PAR2 verifies from the decode
    // buffer and short-circuits the authoritative pass — rev 9's "live PAR2 is
    // load-bearing". After a restart there is no decode buffer: no article of
    // the set arrives, live PAR2 has nothing to hash, the gate read `None`, and
    // the completion gate took its repair branch — which materializes every
    // still-routing set and redownloads a set that was already perfect. Direct
    // RAR sets now contribute `StrongDecode` themselves.
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
    // The contribution is only earned once the gate re-arm has re-read the
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

/// The last holds failure mode: the scratch file cannot be opened at all.
#[tokio::test]
async fn a_scratch_io_failure_demotes_the_set() {
    let member_name = "Silver.Horizon.S01E36.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
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

/// The pause demand, driven through the command seam it is wired at.
#[tokio::test]
async fn pausing_a_job_with_dirty_direct_coverage_demands_a_barrier() {
    let member_name = "Silver.Horizon.S01E37.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
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

/// The checkpoint's per-volume `complete` bit means *all bytes durable*, and
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

/// A restart mid-download of a set's **last** volume.
///
/// The one shape the structural proof cannot reach: the cached facts stopped
/// short of the end-of-archive record, and the volume *closes* the member
/// chain, so `split_after` says nothing about whether a second member's header
/// sits past the first's data area. An earlier shape demoted here by design.
///
/// The expensive arm does the only thing that actually answers the question: it
/// rebuilds the walk's reader out of the volume's **envelope**, which holds every
/// non-member byte at its true physical offset and is therefore exactly the
/// header region, and re-parses. The set finishes one-pass and byte-identically
/// instead of paying a materialization.
#[tokio::test]
async fn a_restored_last_volume_reconfirms_from_its_envelope_and_finalizes() {
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
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the re-parse must confirm the restored last volume rather than demote it, got {shape}"
    );
    // One pass: a demotion is what would have written these.
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "{filename} must never reach disk for a set that stayed direct"
        );
    }
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the reconfirmed set must commit its member byte for byte"
    );
}

/// The other half of the same seam: the re-parse is a *proof*, not a permission.
///
/// With the restored volume's envelope gone, the walk's reader has a hole where
/// the headers were — nothing can prove the tail holds no undiscovered member —
/// and the volume must demote under its own name exactly as it always has. What
/// must not happen is confirming on the strength of "every article arrived",
/// which would file an unproven region into an envelope that finalization
/// deletes.
#[tokio::test]
async fn a_restored_last_volume_whose_envelope_is_gone_still_demotes_by_name() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E42.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41082);
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
    // The last volume's envelope, gone from under an already-validated
    // checkpoint. Deleted *after* the restore so the row itself is untouched and
    // this isolates the re-parse: the remaining articles recreate the file, but
    // only with the tail bytes they carry, so the header region the walk has to
    // read is a hole.
    let envelope = working_dir.join("silver.horizon.f0.vol00002.envelope");
    assert!(
        envelope.exists(),
        "non-vacuity: the volume must have had an envelope to lose"
    );
    crate::pipeline::release_cached_write_handle(&envelope);
    std::fs::remove_file(&envelope).unwrap();

    for (file_index, segment_number) in peek_queued_segments(&mut pipeline, job_id) {
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
        "a restored volume the re-parse cannot run over must demote under its own \
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
    assert!(
        requeued.contains(&(2, 0))
            && requeued.contains(&(2, 3))
            && !requeued.contains(&(2, 1))
            && !requeued.contains(&(2, 2)),
        "only the restored volume's unmaterialized edge articles should return to conventional ownership, got {requeued:?}"
    );
    for (file_index, (filename, bytes)) in volumes.iter().enumerate() {
        let Some(on_disk) = std::fs::read(working_dir.join(filename)).ok() else {
            continue;
        };
        assert!(
            on_disk.len() <= bytes.len(),
            "{filename} must not grow past its canonical geometry"
        );
        for segment_number in 0..ARTICLES as u32 {
            if requeued.contains(&(file_index as u32, segment_number)) {
                continue;
            }
            let (start, end) = article_extent(bytes.len(), segment_number, ARTICLES);
            assert!(
                end <= on_disk.len(),
                "{filename} segment {segment_number} stayed materialized, so its full extent must exist"
            );
            assert_eq!(
                &on_disk[start..end],
                &bytes[start..end],
                "{filename} segment {segment_number} stayed materialized, so its bytes must be exact"
            );
        }
    }
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member, None,
        "an unconfirmed volume's set must not commit a member it could not prove"
    );
}

// ---------------------------------------------------------------------------
// Repair while still direct
// ---------------------------------------------------------------------------

/// A PAR2 index over the set's decoded volume bytes that also carries
/// **recovery blocks**, so the damage it describes can actually be repaired.
///
/// `build_test_par2_index_for_files` stops at descriptions and slice checksums,
/// which is why every damaged-set test before repair landed could only assert a
/// verdict: with no recovery stream neither gate can repair, so "repairs while
/// direct" had nothing to compare against. The blocks are computed over the
/// global input-slice ordering PAR2 defines — files in main-packet order,
/// slices in order within each file, each padded to `slice_size` — which is the
/// same ordering `plan_repair` reconstructs from the parsed set.
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
    let constants = par2_rs::input_slice_constants(slice_count);

    for exponent in 0..recovery_block_count as u32 {
        let mut recovery = vec![0u8; slice_size_bytes];
        for (input_index, &constant) in constants.iter().enumerate() {
            let factor = par2_rs::gf_pow(constant, exponent);
            for word_index in 0..word_count {
                let at = input_index * slice_size_bytes + word_index * 2;
                let input_word = u16::from_le_bytes([padded[at], padded[at + 1]]);
                let contribution = par2_rs::gf_mul(input_word, factor);
                let current =
                    u16::from_le_bytes([recovery[word_index * 2], recovery[word_index * 2 + 1]]);
                let updated = par2_rs::gf_add(current, contribution).to_le_bytes();
                recovery[word_index * 2] = updated[0];
                recovery[word_index * 2 + 1] = updated[1];
            }
        }
        let mut body = Vec::with_capacity(4 + slice_size_bytes);
        body.extend_from_slice(&exponent.to_le_bytes());
        body.extend_from_slice(&recovery);
        stream.extend_from_slice(&build_test_par2_packet(
            par2_rs::packet::header::TYPE_RECOVERY,
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

/// [`par2_bearing_job_spec`] with the index placed at either end of the NZB, and
/// a chosen number of articles per volume.
fn par2_bearing_job_spec_positioned(
    name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
    articles: usize,
) -> (JobSpec, u32) {
    let mut spec = direct_store_job_spec_with_articles(name, volumes, articles);
    let file_index = append_par2_index(&mut spec, par2_bytes);
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
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_repairable_par2_gate_at(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
    password: Option<&str>,
) -> RepairGateOutcome {
    run_repairable_par2_gate_with_articles(
        gate,
        job_id,
        member_name,
        volumes,
        par2_bytes,
        position,
        password,
        2,
    )
    .await
}

/// [`run_repairable_par2_gate_at`] with a chosen number of articles per volume.
///
/// One article per volume is not a corner case — a volume small enough to post
/// whole is ordinary — and it is the only shape in which a repair's rewrite,
/// which is widened to whole articles, reaches a volume's **first** byte and
/// therefore the first cipher block of a member extent that starts there. With
/// two articles the damaged one is always bounded away from at least one of the
/// extent's edges.
#[allow(clippy::too_many_arguments)]
async fn run_repairable_par2_gate_with_articles(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
    password: Option<&str>,
    articles: usize,
) -> RepairGateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec_positioned("Silver Horizon", volumes, par2_bytes, position, articles);
    spec.password = password.map(str::to_owned);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32)
        .flat_map(|ordinal| (0..articles as u32).map(move |segment| (ordinal, segment)))
        .collect();
    let mut volume_file_seen = false;
    for (ordinal, segment_number) in arrivals {
        submit_volume_article_indexed_of(
            &mut pipeline,
            job_id,
            volumes,
            ordinal,
            position.volume_file_index(ordinal),
            segment_number,
            articles,
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
            .or_else(|| staging_member(&complete_dir, member_name))
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
    // The first transition, end to end. The damaged byte is in a recovery
    // record's data area: outside every member's packed range, so neither the
    // per-part packed CRC32 nor the whole-member CRC32 covers it, and inside a
    // service block's data rather than a header, so the walk still parses and
    // the volume still confirms. PAR2 is the only layer that can see it — which
    // is exactly the set-up an earlier shape could only demote on.
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
    // The index-space regression, and the reason every other repair fixture is
    // blind to it. A repair materializes its damaged volumes through the hybrid
    // provider, and that provider is keyed by **job file index** so one
    // instance can answer for every set of a job. The reconstruction plan was
    // built with the set's own **volume index**. The two are the same number
    // exactly when a set's volumes are NZB files `0..n-1` — true whenever the
    // PAR2 is appended last, which is every fixture and most real NZBs, and
    // false the moment a `.par2` or `.nfo` leads or a job carries two sets.
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
        None,
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
    // repaired, so repair refuses before it materializes anything and the
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
    // The damage sits entirely in volume 1's RAR recovery record; the member's
    // own data slices are untouched. Once the demotion hands its materialized
    // volumes back to the conventional completion seam, extraction reads the
    // intact data and delivers the member byte for byte — the damaged
    // recovery record is archive residue the job never needed. This test once
    // asserted the job must NOT complete, but that pinned an accident: the
    // demoted volumes never entered the archive topology at all, so
    // extraction never had the chance to prove the data was fine. Delivering
    // provably intact content is the outcome the whole pipeline exists for.
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the member's data slices are undamaged, so the conventional path must \
         deliver it byte for byte; sets = {}",
        direct.sets
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "with the member delivered intact, the job completes; the unrepairable \
         damage lived only in a recovery record nothing needed, got {:?}",
        direct.status
    );
}

/// A par2-bearing direct job driven to the point where its one set is live and
/// carries repairable PAR2 damage, with the live pipeline handed back so a test
/// can drive the repair seam itself and watch what it refuses.
async fn live_damaged_direct_job(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    holds_budget: Option<u64>,
) -> (Pipeline, PathBuf) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
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
    // download pipeline never looks exhausted and the settle guard defers every
    // verdict.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    (pipeline, working_dir)
}

/// The envelope-damage fixture every repair test is built on: three volumes
/// carrying one stored member and a recovery record, with the record's data
/// area damaged in the middle volume.
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
    // Every repaired byte re-enters the router as a hold, so a rewrite is
    // charged against the same RAM ceiling ordinary staging is — and the first
    // shape never checked: it read every rewrite span of every damaged volume
    // whole, then let the router copy them into staging, so a missing-volume
    // repair of a large set peaked at about twice the repaired bytes with
    // nothing bounding either term.
    //
    // The A/B is the budget and nothing else. A later pass revisits the bound
    // itself — routing a repaired volume in budget-sized instalments lifts it —
    // but until then an over-budget rewrite demotes, and it must do so before
    // the checkpoint delete, so finding out costs the set nothing.
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
    // Nothing else terminates the loop. A set that is damaged again after a
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
    //
    // Placed inside volume 1's byte range (`chunk` is 800 bytes here, so
    // [800, 1600) is volume 1 — the one `repairable_envelope_damage` already
    // damaged and repaired), not at a fixed low offset. The post-repair pass
    // is selective now: it reads back only the volumes the repair rewrote and
    // carries every other volume's pre-repair verdict forward unread — the
    // same trust class the conventional selective pass already accepts for
    // bytes that move outside its own write set. Damage anywhere else in the
    // set would not be caught by this pass and would prove nothing about the
    // bound this test exists to pin.
    let partial = std::fs::read_dir(payload_root(&temp_dir, JobId(41103)))
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".direct.partial"))
        })
        .expect("the live set still holds its member partial");
    let mut bytes = std::fs::read(&partial).unwrap();
    bytes[810] ^= 0xFF;
    std::fs::write(&partial, &bytes).unwrap();

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let mut first_pending_work = None;
    let resolution = loop {
        let resolution = pipeline
            .resolve_direct_sets_before_par2_repairer(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone(),
            )
            .await;
        if !matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ) {
            break resolution;
        }
        if first_pending_work.is_none() {
            let pending = pipeline
                .direct_post_repair_in_flight
                .get(&job_id)
                .expect("the first check must leave one post-repair ticket in flight");
            first_pending_work = Some((pending.work_id, pending.recovery_set_id));

            let duplicate = pipeline
                .resolve_direct_sets_before_par2_repairer(
                    job_id,
                    Arc::clone(&par2_set),
                    working_dir.clone(),
                )
                .await;
            assert!(
                matches!(
                    duplicate,
                    crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
                ),
                "a repeated completion check must reuse the in-flight ticket; got {duplicate:?}"
            );
            assert_eq!(
                pipeline
                    .direct_post_repair_in_flight
                    .get(&job_id)
                    .map(|work| (work.work_id, work.recovery_set_id)),
                first_pending_work,
                "the repeated check must not replace or duplicate the ticket"
            );
            pipeline.remove_pending_completion_check(job_id);
            pipeline.schedule_job_completion_check_if_download_pipeline_drained(
                job_id,
                "post-repair ticket test",
            );
            assert!(
                !pipeline.pending_completion_checks.contains(&job_id),
                "normal drain ticks must not poll an in-flight post-repair ticket"
            );

            let (work_id, recovery_set_id) = first_pending_work.unwrap();
            pipeline.handle_direct_post_repair_done(crate::pipeline::DirectPostRepairWorkDone {
                job_id,
                work_id: work_id.wrapping_add(1),
                recovery_set_id,
                result: Err("stale verdict".to_string()),
            });
            assert!(
                !pipeline.direct_post_repair_results.contains_key(&job_id),
                "a stale ticket must not publish a verdict"
            );
        }
        let done = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.direct_post_repair_done_rx.recv(),
        )
        .await
        .expect("the post-repair read-back should finish")
        .expect("the post-repair completion channel stays open");
        pipeline.handle_direct_post_repair_done(done);
    };

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Unresolved
        ),
        "a set that has had its attempt must fall through to the demotion rather \
         than report a repair; got {resolution:?}; sets = {sets}"
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

// ---------------------------------------------------------------------------
// Waiting for targeted recovery instead of demoting
// ---------------------------------------------------------------------------

/// The same envelope damage, with the recovery split out of the index and into
/// a **separate recovery volume** — which is where recovery actually lives.
///
/// This is the shape every real damaged job has and no earlier fixture did.
/// `recovery_blocks_available` counts slices that have been *merged*, and a
/// recovery volume is only fetched once damage is known, so at the moment the
/// first damage verdict is reached the merged count is structurally zero. Every
/// fixture that baked the recovery into the index handed the repair blocks it
/// would never have had in the field.
type RecoveryVolumeFixture = (Vec<(String, Vec<u8>)>, Vec<u8>, Vec<u8>);

fn recovery_in_a_separate_volume(
    member_name: &str,
    payload: &[u8],
    recovery_blocks: usize,
    damaged_volumes: &[usize],
) -> RecoveryVolumeFixture {
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, payload, 3, rr_bytes);
    // The index describes the set and carries no recovery of its own; the
    // volume carries the same description *plus* the blocks, which is how a
    // real `.volNNN+CC.par2` is laid out.
    let index_bytes = repairable_par2_index(&clean, 0);
    let recovery_bytes = repairable_par2_index(&clean, recovery_blocks);
    let mut volumes = clean;
    for volume in damaged_volumes {
        damage_recovery_record(&mut volumes, *volume, rr_bytes);
    }
    (volumes, index_bytes, recovery_bytes)
}

/// Appends a PAR2 **recovery volume** to a spec as one more downloadable file,
/// and returns its NZB index.
///
/// The name is the payload: `recovery_block_count` is parsed straight out of
/// `.volNNN+CC.par2`, and that parse is the whole of the job's advertised
/// recovery capacity before a single recovery byte has been fetched. Nothing
/// delivers this file — that is the point of the fixture.
fn append_par2_recovery_volume(spec: &mut JobSpec, filename: &str, bytes: &[u8]) -> u32 {
    let file_index = spec.files.len() as u32;
    spec.total_bytes += u64::from(yenc_declared_bytes(bytes.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(filename),
        filename: filename.to_string(),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(bytes.len() as u32),
            message_id: "direct-par2-recovery@example.com".to_string(),
        }],
    });
    file_index
}

const RECOVERY_VOLUME_NAME: &str = "silver.horizon.vol000+04.par2";

/// A live, damaged direct job whose recovery is advertised in the NZB and has
/// **not** been downloaded — the state every damaged direct set is really in
/// when its first verdict lands.
///
/// Returns the pipeline, the working directory, and the two PAR2 file indices —
/// the index, which each test delivers itself because delivering it *is* the
/// moment the damage verdict happens, and the recovery volume, which is what the
/// wait is waiting for.
///
/// Stops one step short of the verdict on purpose. Every test here is about what
/// happens at that instant, and half of them need to change the job's state
/// first: empty the recovery pool, spend the defer budget, leave the payload in
/// flight.
async fn direct_job_with_undownloaded_recovery(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    index_bytes: &[u8],
    recovery_volume_name: &str,
    recovery_volume_bytes: &[u8],
) -> (Pipeline, PathBuf, u32, u32) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", volumes, index_bytes);
    let recovery_file_index =
        append_par2_recovery_volume(&mut spec, recovery_volume_name, recovery_volume_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let set_id = par2_rs::Par2FileSet::from_files(&[index_bytes])
        .expect("fixture index parses")
        .recovery_set_id;
    let recovery = pipeline
        .ensure_par2_runtime(job_id)
        .files
        .entry(recovery_file_index)
        .or_default();
    recovery.filename = recovery_volume_name.to_string();
    recovery.discovery = Par2DiscoveryState::PrefixProbed {
        set_ids: vec![set_id],
    };
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
    }
    // The harness delivers articles without ever dequeuing them, so the payload
    // would otherwise never look settled and the settle guard would defer every
    // verdict. Only the ordinary queue is cleared: the *parked* recovery pool is
    // what targeted promotion selects from, it is deliberately excluded from
    // "pending download work", and emptying it here would quietly turn every
    // test below into the exhausted case.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
    }
    (pipeline, working_dir, index_file_index, recovery_file_index)
}

/// Delivers the PAR2 index, which is what produces the damage verdict and drives
/// the completion gate into the direct-aware seam. Nothing here is a test hook:
/// this is the ordinary decode path a real index arrives on.
async fn deliver_par2_index(
    pipeline: &mut Pipeline,
    job_id: JobId,
    index_file_index: u32,
    index_bytes: &[u8],
) {
    submit_decoded_segment(
        pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        index_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
}

/// What the quiet direct pass concluded, as `(blocks_needed, blocks_available)`.
fn insufficient_verdict(pipeline: &Pipeline) -> Option<(u32, u32)> {
    match pipeline.last_direct_verdict.as_ref()?.repairable {
        par2_rs::verify::Repairability::Insufficient {
            blocks_needed,
            blocks_available,
            ..
        } => Some((blocks_needed, blocks_available)),
        _ => None,
    }
}

#[tokio::test]
async fn damage_needing_undownloaded_recovery_waits_instead_of_demoting() {
    // The bug this whole path was written around, in one assertion. Recovery is
    // fetched only once damage is known, so the first damage verdict of a job's
    // life always reads zero merged recovery blocks — and any damage at all
    // exceeds zero. The set therefore declined, demoted, materialized every
    // volume, and the recovery it was about to receive repaired physical files
    // instead. No fixture could reach the in-place repair, because the input it
    // needs is guaranteed absent at the only moment it was consulted.
    //
    // So the verdict is answered by *waiting*: ask for the recovery the damage
    // needs and stay direct until it lands.
    let member_name = "Silver.Horizon.S02E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41111);
    let (mut pipeline, working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    // Demanded rather than waited for: the checkpoint row is written on a timer,
    // and the claim below — that waiting costs the set nothing durable — is
    // vacuous if there was no row to keep.
    pipeline
        .demand_direct_store_barriers(job_id, BarrierDemand::PhaseChange)
        .await;
    let coverage_before = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(
        !coverage_before.is_empty(),
        "non-vacuity: the live set must hold a checkpoint row before the verdict, \
         or 'the row survives' proves nothing"
    );

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        insufficient_verdict(&pipeline),
        Some((1, 0)),
        "non-vacuity: the pass must reach the verdict this exists for — damage \
         that needs blocks, with none merged yet; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_defers, 1,
        "the set must wait for the recovery rather than answer with what it has; \
         sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "and it must still be direct while it waits — a demotion here throws \
         away the outputs the wait exists to keep; got {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.repair_attempted()),
        "the once-latch must be intact: the deferred pass has to be the set's \
         *first* real attempt, or the retry refuses with AlreadyRepaired and \
         demotes for the verdict the recovery was about to answer; got {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "nothing irreversible may have run — no checkpoint delete, no \
         materialization; sets = {sets}"
    );
    assert_eq!(
        pipeline.db.load_direct_coverage(job_id).unwrap(),
        coverage_before,
        "the checkpoint row is deleted by an attempt, and no attempt was made"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no scratch: nothing was materialized"
    );
    assert!(
        pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and the wait must have asked for something — the recovery volume that \
         covers the damage is now promoted"
    );
    assert!(pipeline.jobs.get(&job_id).is_some_and(|state| {
        state.download_queue.count_matching(|work| {
            work.segment_id.file_id.file_index == recovery_file_index && work.completion_critical
        }) > 0
    }));
    assert!(
        pipeline
            .list_jobs()
            .into_iter()
            .find(|job| job.job_id == job_id)
            .is_some_and(|job| job.fetching_repair_data),
        "the public job snapshot must expose the completion-critical repair fetch"
    );
    assert!(
        pipeline.job_has_promoted_recovery_pipeline_work(job_id, "test"),
        "with its work on the wire, which is what makes the wait bounded"
    );

    // Every later tick of the gate while that work is on the wire has to answer
    // without verifying anything. The pass is a full PAR2 scan, the gate ticks
    // on every completing article, and until the wave merges the scan can only
    // reach the verdict that started the wait.
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let before = pipeline.direct_session_pass_calls;
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Deferred
        ),
        "a job already waiting stays waiting; got {resolution:?}"
    );
    assert_eq!(
        pipeline.direct_session_pass_calls, before,
        "and does not re-verify to find that out"
    );
}

#[tokio::test]
async fn a_deferred_direct_set_repairs_in_place_once_its_recovery_lands() {
    // The other half, and the only one that proves the feature reachable: the
    // wait has to end in an in-place repair. A completing PAR2 file already
    // merges its slices and re-checks the job, so nothing new re-arms the gate —
    // the set simply has to still be direct when that happens.
    let member_name = "Silver.Horizon.S02E02.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 179) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41112);
    let (mut pipeline, working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;
    assert_eq!(
        pipeline.direct_store.repair_defers, 1,
        "non-vacuity: act one has to be the wait, or act two proves nothing"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and the set must arrive at act two with its attempt unspent"
    );

    // The promoted work leaves the queue as it is picked up, which the harness
    // does not model — it delivers articles without ever dequeuing them. Left
    // in, the wave reads as permanently in flight and the gate rightly refuses
    // to re-verify while it is.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
    }

    // Act two: the promoted recovery volume arrives. This is the production
    // re-arm and nothing else — the decode seam merges the slices and runs the
    // completion gate, which is where the repair now finds non-zero recovery.
    let mut repair_events = pipeline.event_tx.subscribe();
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: recovery_file_index,
        },
        0,
        0,
        &recovery_bytes,
        RECOVERY_VOLUME_NAME,
        None,
    )
    .await;

    // The repair never enters `JobStatus::Repairing`, so the event pair is the
    // only public record it ran — a consumer reading this job's history must
    // see a repair, not a job that was never damaged.
    let announced = drain_job_events(&mut repair_events, job_id);
    assert_eq!(
        announced
            .iter()
            .filter(|event| matches!(event, PipelineEvent::RepairStarted { .. }))
            .count(),
        1,
        "the in-place repair must announce itself; got {announced:?}"
    );
    assert_eq!(
        announced
            .iter()
            .filter(|event| matches!(
                event,
                PipelineEvent::RepairComplete {
                    slices_repaired: 1,
                    ..
                }
            ))
            .count(),
        1,
        "and must report completion with the one slice it rebuilt; got {announced:?}"
    );

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_materialized_volumes
        ),
        (1, 1),
        "the set must repair in place once the blocks are merged, materializing \
         only the damaged volume; sets = {sets}"
    );
    assert_eq!(
        pipeline
            .metrics
            .direct_sets_repaired_while_direct
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "and the lifetime counter — which had never read non-zero — must count \
         it; sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "the set stays direct throughout; got {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.repair_attempted()),
        "with its one attempt now spent, which is what bounds a set that comes \
         back damaged; got {sets}"
    );
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "and no source volume may exist under its own name — the repair reads \
         every clean volume virtually"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "the scratch is deleted once its spans are routed"
    );
}

#[tokio::test]
async fn a_direct_set_demotes_when_the_recovery_it_needs_cannot_arrive() {
    // The livelock guard, and the reason the wait is decided rather than
    // assumed. Waiting is only correct while recovery is actually coming: with
    // the recovery articles gone there is nothing to promote and nothing on the
    // wire, so the answer has to be the immediate demotion it always was. This
    // branch has produced two livelocks already; an unbounded wait would be the
    // third.
    let member_name = "Silver.Horizon.S02E03.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41113);
    let (mut pipeline, _working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;
    // The one difference from the deferring run: the recovery work is gone, as
    // it is for a job whose recovery articles came back unavailable. The blocks
    // are still advertised in the NZB, so capacity still covers the damage — the
    // only thing missing is any way to fetch them.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.recovery_queue = crate::DownloadQueue::new();
    }

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap())
            >= 1,
        "non-vacuity: the NZB still advertises enough recovery, so the demotion \
         below is about the recovery being unreachable and nothing else"
    );
    assert_eq!(
        pipeline.direct_store.repair_defers, 0,
        "the damage must be answered by demoting rather than waiting; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and decided before any attempt: an insufficient verdict is one the \
         planner refuses outright, so an attempt could only burn the latch and \
         the checkpoint row on its way to the same refusal; sets = {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "there was nothing left to promote, which is exactly why it demotes"
    );
    assert!(
        sets.contains("Demoted(Par2Damaged)"),
        "the set materializes for the conventional path, which reaches the same \
         dead end with better diagnostics; got {sets}"
    );
}

#[tokio::test]
async fn the_defer_budget_bounds_a_job_that_keeps_coming_back_short() {
    // The arithmetic bound behind the structural one. The first wave asks for
    // every block the verdict needs, so a second only happens if the first
    // arrived and still fell short — but "already promoted" is derived state,
    // and a derivation that goes wrong here waits forever. Budget spent, the
    // next short verdict demotes even with recovery still parked and ready to
    // promote.
    let member_name = "Silver.Horizon.S02E04.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41114);
    let (mut pipeline, _working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;
    // Three waves already spent. Everything else is exactly the run that waits.
    pipeline
        .direct_store
        .set_repair_defer_waves(job_id, MAX_DIRECT_REPAIR_DEFER_WAVES);

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        pipeline.direct_store.repair_defers, 0,
        "a job past its budget must not start another wait; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and the demotion is decided without spending an attempt on a verdict \
         the planner would refuse; sets = {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and must not promote another wave of recovery to wait for"
    );
    assert!(
        sets.contains("Demoted(Par2Damaged)"),
        "it demotes instead; got {sets}"
    );
}

#[tokio::test]
async fn damage_beyond_every_advertised_recovery_block_never_waits() {
    // Waiting can only ever help damage the recovery *set* could cover.
    // `blocks_available` is what has merged; the NZB's advertised total is the
    // ceiling, and past it no amount of downloading changes the answer. Delaying
    // the conventional path there helps nobody: it reaches the same dead end,
    // with the diagnostics that name it.
    //
    // Driven at the decision itself rather than through a fixture, because the
    // damage a small fixture can produce cannot outrun the advertised capacity
    // the same fixture implies — and this contract is about one comparison, not
    // about how damage is reached. The call is the production one; only the
    // block count is chosen.
    let member_name = "Silver.Horizon.S02E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41115);
    let (mut pipeline, _working_dir, _index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    let par2_set =
        par2_rs::Par2FileSet::from_files(&[&index_bytes]).expect("fixture index must parse");
    let recovery_set_id = par2_set.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let capacity = pipeline.total_recovery_block_capacity(job_id, recovery_set_id);
    assert!(
        capacity > 0,
        "non-vacuity: the job must advertise recovery, so the refusal below is \
         about the comparison and not about there being nothing to ask for"
    );

    assert!(
        !pipeline.defer_direct_repair_for_recovery(job_id, capacity + 1, 0),
        "one block past everything the NZB advertises, and no download can close \
         the gap: the answer has to be the immediate demotion"
    );
    assert_eq!(pipeline.direct_store.repair_defers, 0, "nothing waited");
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and nothing was fetched for a wait that could never end — a refusal here \
         must cost no bandwidth at all"
    );

    // The A/B, on the same job: one block fewer is inside the advertised
    // capacity, and that alone flips the answer.
    assert!(
        pipeline.defer_direct_repair_for_recovery(job_id, capacity, 0),
        "non-vacuity: at the ceiling the same call waits, so the ceiling is the \
         only thing under test"
    );
    assert!(
        pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and asks for the recovery it means to wait for"
    );
}

#[tokio::test]
async fn a_direct_set_still_receiving_articles_neither_repairs_nor_waits_nor_demotes() {
    // The settle guard, unchanged and now load-bearing twice over. A set with
    // articles outstanding has holes where its missing ranges will go, and PAR2
    // cannot tell a hole from corruption. Repairing there spends recovery blocks
    // rebuilding bytes already on their way — and *waiting* there is worse,
    // because it fetches those blocks first. The verdict is not yet evidence of
    // anything, so nothing acts on it.
    let member_name = "Silver.Horizon.S02E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41116);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &index_bytes);
    let recovery_file_index =
        append_par2_recovery_volume(&mut spec, RECOVERY_VOLUME_NAME, &recovery_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // Every article but the last volume's tail. The set is live, damaged where
    // the fixture damaged it, and genuinely incomplete everywhere else — and the
    // ordinary download queue is left alone, so the job reads as still
    // downloading, which is the truth.
    let mut arrivals = in_order_arrivals(volumes.len());
    arrivals.pop();
    for (file_index, segment_number) in arrivals {
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
        &index_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let verification = pipeline
        .verify_direct_sets_quietly(job_id, par2_set, working_dir.clone())
        .await
        .expect("the quiet pass reached a verdict");
    assert!(
        verification.needs_repair(),
        "non-vacuity: the pass must see damage, or every guard below is trivially \
         satisfied"
    );
    let resolution = pipeline
        .resolve_direct_sets_with_par2_damage(job_id, &verification)
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        resolution,
        crate::pipeline::direct_store::wiring::DirectDamageResolution::Unresolved,
        "an unsettled set answers nothing; sets = {sets}"
    );
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_defers
        ),
        (0, 0),
        "it neither repairs nor waits; sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "nor demotes — the bytes it is missing are on their way; got {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and no recovery is promoted to rebuild bytes that are already coming, \
         which is the bandwidth the deferred recovery fetch exists to save"
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

/// Envelope files still sitting in `working_dir`.
///
/// Top level only, and by suffix, which is enough here and deliberately not
/// enough in production: `sweep_orphan_direct_files` walks eight levels into a
/// tree the *archive* names, where `chapter.envelope` is a file a real archive
/// can perfectly well contain. These fixtures name their own members.
fn direct_envelopes_left(working_dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(working_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".envelope"))
        .count()
}

#[tokio::test]
async fn two_sets_sharing_a_clamped_partial_keep_their_bytes_apart() {
    // Internal direct-store paths were derived from names alone, and
    // `path_component_with_suffix` clamps a long
    // name's stem to fit `DOWNLOAD_FILENAME_MAX_BYTES` — so two members whose
    // names differ only past the clamp point reached the *same*
    // `.direct.partial` while their final destinations stayed distinct. Both
    // routers then wrote one file, each passed its integrity gates over its own
    // in-memory buffers, and one member's bytes were silently wrong on disk.
    //
    // The shape is deliberate in two ways. The names share their first 226
    // bytes and differ only in the tail, because the collision under test is
    // the clamp, not equality: members with *equal* names also collide on the
    // final destination, where last-writer-wins is the same semantic two
    // conventionally extracted archives already have — bytes can never witness
    // that. Distinct finals are what make each set's output independently
    // assertable. And the arrival order interleaves the two sets (the spec
    // builder already orders the files that way), because sequential sets
    // merely time-slice a shared path — each finalize renames it away before
    // the neighbour writes — and the corruption needs both sets holding it at
    // once.
    let stem: String = format!("Silver.Horizon.{}", "x".repeat(211));
    let member_a = format!("{stem}-alpha.mkv");
    let member_b = format!("{stem}-omega.mkv");
    let payload_a: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let payload_b: Vec<u8> = (0..2500u32).map(|index| (97 + index % 101) as u8).collect();
    let set_a = single_member_store_set(&member_a, &payload_a, 2);
    let set_b = renamed_set(
        "amber.trail",
        single_member_store_set(&member_b, &payload_b, 2),
    );
    let volumes: Vec<(String, Vec<u8>)> = set_a.iter().chain(set_b.iter()).cloned().collect();
    let job_id = JobId(41120);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        for (filename, _) in &volumes {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    assert_eq!(
        pipeline.direct_store.sets_for(job_id).len(),
        2,
        "the fixture must admit two direct sets or it is testing nothing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    assert_eq!(
        pipeline.direct_store.finalized_sets, 2,
        "both sets must finalize direct; under a shared partial one set's \
         commit finds its bytes gone or mixed and demotes"
    );
    assert!(!volume_file_seen, "no source volume may ever materialize");
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(&member_a)).ok().as_deref(),
        Some(payload_a.as_slice()),
        "set A's member must arrive whole under its own name"
    );
    assert_eq!(
        std::fs::read(output_root.join(&member_b)).ok().as_deref(),
        Some(payload_b.as_slice()),
        "set B's member must arrive whole under its own name"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a job whose sets collide only inside the clamp still completes"
    );
}

/// One recovery set over two direct sets, one of which finalizes while the other
/// is still damaged.
struct TwoSetPar2Fixture {
    /// Both sets' volumes, in NZB order: set A first, then set B.
    volumes: Vec<(String, Vec<u8>)>,
    /// Set B's volumes alone, for the assertions about what must not appear
    /// under a live volume's own name.
    live_set: Vec<(String, Vec<u8>)>,
    par2_bytes: Vec<u8>,
    /// The `(file index, segment number)` that never arrives.
    lost: (u32, u32),
}

/// Builds the quiet-pass fixture.
///
/// Set A is clean and gate-passed. Set B loses the second half of its middle
/// volume — member payload, recovery record and end-of-archive record together —
/// which is what keeps its member gate open so it cannot finalize alongside its
/// neighbour, and what leaves damage only PAR2 can answer.
fn two_set_par2_fixture(
    finalized_member: &str,
    live_member: &str,
    finalized_payload: &[u8],
    live_payload: &[u8],
) -> TwoSetPar2Fixture {
    let finalized_set = single_member_store_set(finalized_member, finalized_payload, 2);
    let live_set = renamed_set(
        "amber.trail",
        recovery_record_store_set(live_member, live_payload, 3, 256),
    );
    let volumes: Vec<(String, Vec<u8>)> = finalized_set
        .iter()
        .chain(live_set.iter())
        .cloned()
        .collect();
    let par2_bytes = repairable_par2_index(&volumes, 16);
    let lost = (finalized_set.len() as u32 + 1, 1u32);
    TwoSetPar2Fixture {
        volumes,
        live_set,
        par2_bytes,
        lost,
    }
}

/// Drives the quiet-pass fixture to the state the capability lives in: every
/// article but the lost one delivered, the PAR2 index parsed, the download
/// pipeline drained, and set A finalized while set B is still live and damaged.
///
/// Reaching the state directly, because it is a *state*, not a sequence: a job
/// whose PAR2 already read clean once released its ready sets, and one of them
/// being ready while the other is not is the whole shape. Later passes happen
/// for reasons that have nothing to do with the direct sets — a conventional
/// member failing extraction is enough.
async fn direct_job_with_one_finalized_neighbour(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    lost: (u32, u32),
) -> PathBuf {
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    let working_dir = insert_active_job(pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == lost {
            continue;
        }
        submit_volume_article(pipeline, job_id, volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        pipeline,
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
    // The lost article is never coming, so the pass may treat holes as damage.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }

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
    working_dir
}

#[tokio::test]
async fn a_finalized_set_does_not_stop_its_live_neighbour_repairing_while_direct() {
    // Second round: the capability itself, where the first round could only
    // assert the absence of the bug it fixed.
    //
    // Round one fixed the **attribution**. The quiet pass repair runs in front
    // of the repairer was a bare `verify_all`, without the two damage
    // adjustments the authoritative pass applies to its own verdict, so a
    // finalized set's absent volumes all read `Missing`, `damaged_files_by_set`
    // found no live owner for them and refused the whole attempt with
    // `DamageOutsideDirectSets` — and the set that *was* live and *was*
    // repairable demoted instead, for damage belonging to files the job
    // legitimately finished without. After it, the pass reached the right set.
    //
    // It still could not repair it, and that is what this test is now about.
    // PAR2 repair is Reed–Solomon over the whole recovery set: rebuilding one
    // slice reads the surviving slice of *every other file* at the same index,
    // the finalized set's volumes included — and those had no image left, their
    // partials renamed to their destinations and their envelopes deleted, so
    // `execute_repair` failed on the first one it could not open. The two halves
    // were mutually exclusive: stage the finalized set's volumes and the bug
    // round one fixed does not trigger; leave them absent and the neighbour
    // cannot repair. The old assertion was the honest end state of that
    // stalemate — an attempt was made, and it failed.
    //
    // Retention closes it. A set finalizing beside a live neighbour keeps its
    // envelopes and re-points its member extents at the committed destinations
    // — the same bytes, because a commit is a rename — so it stays readable for
    // exactly as long as something in this job can ask.
    let finalized_member = "Silver.Horizon.S01E27.mkv";
    let live_member = "Amber.Trail.S01E01.mkv";
    let finalized_payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 227) as u8).collect();
    let TwoSetPar2Fixture {
        volumes,
        live_set,
        par2_bytes,
        lost,
    } = two_set_par2_fixture(
        finalized_member,
        live_member,
        &finalized_payload,
        &live_payload,
    );

    // The reference: the same job, the same lost article, the conventional
    // repairer over real volume files. What the direct run has to match.
    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41090),
        live_member,
        &volumes,
        &par2_bytes,
        lost,
        None,
    )
    .await;
    assert_eq!(
        conventional.member.as_deref(),
        Some(live_payload.as_slice()),
        "non-vacuity: the gate-off reference must repair the lost article and \
         extract the damaged set's member, or there is nothing to be identical \
         to; status={:?}",
        conventional.status
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41091);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let working_dir =
        direct_job_with_one_finalized_neighbour(&mut pipeline, job_id, &volumes, &par2_bytes, lost)
            .await;
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "non-vacuity for the retention itself: the finalized set must still own \
         its envelopes here, or the repair below is reading nothing it would not \
         have had anyway"
    );
    assert_eq!(
        std::fs::read(payload_root(&temp_dir, job_id).join(finalized_member)).ok(),
        Some(finalized_payload.clone()),
        "and its member must be committed at its destination — the staging root, \
         not the working directory — because that is what the retained image \
         reads the member extents back out of"
    );

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Repaired
        ),
        "the live set's damage is its own and its inputs are all readable — the \
         finalized neighbour's through its retained envelopes and committed \
         members — so the repair must succeed in place. `Unresolved` is the old \
         stalemate: attributed correctly, then `execute_repair` failing on a \
         source volume nothing could open. got {resolution:?}; sets = {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_finalized() || !set.repair_attempted()),
        "and the finalized set is never the one repaired: it is a repair \
         *source*, never a target — nothing may write into a set whose members \
         are already committed. sets = {sets}"
    );
    assert!(
        live_set
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "nothing may materialize under a live volume's own name: that path \
         belongs to demotion"
    );

    // From here it is an ordinary finish: the repaired set re-verifies, its
    // member gate re-arms, it finalizes, and the job completes.
    let mut envelopes_when_all_terminal = None;
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
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
        // Sampled at the first moment both sets are committed, which is where
        // the job's PAR2 story concludes and the retention window shuts. Read
        // here rather than after the loop because a completed job's working
        // directory is cleaned up, and an assertion over a directory that is not
        // there passes for the wrong reason.
        if envelopes_when_all_terminal.is_none() && pipeline.direct_store.finalized_sets >= 2 {
            envelopes_when_all_terminal = Some(direct_envelopes_left(&working_dir));
        }
    }

    let status = job_status_for_assert(&pipeline, job_id);
    let (member, from) = member_after_gate(&complete_dir, &working_dir, live_member);
    assert_eq!(
        member.as_deref(),
        conventional.member.as_deref(),
        "the repaired member must be byte-identical to the gate-off run over the \
         same damage; status={status:?} found_in={from:?}"
    );
    assert_eq!(
        member_after_gate(&complete_dir, &working_dir, finalized_member).0,
        Some(finalized_payload),
        "and the finalized neighbour's own output must be untouched by having \
         been read as a repair source"
    );
    assert_eq!(
        envelopes_when_all_terminal,
        Some(0),
        "once both sets are committed nothing in this job can ask for a virtual \
         volume again, so every retained envelope must be gone — promptly, and \
         without waiting for the working directory to be cleaned up"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "and the repair scratch dies with the repair, as it always has"
    );
}

#[tokio::test]
async fn a_job_that_dies_inside_the_retention_window_sweeps_its_envelopes_on_restart() {
    // Retention is in-memory bookkeeping over files on disk, and nothing about
    // it is persisted — deliberately, because there is nothing a restart could
    // do with it. A finalized set retires its checkpoint row, so restore
    // rebuilds it fresh, claims none of its envelopes, and the orphan sweep
    // takes every one of them. The point of the test is that deferring the
    // delete did not quietly move an envelope out of the sweep's reach.
    let finalized_member = "Silver.Horizon.S01E28.mkv";
    let live_member = "Amber.Trail.S01E02.mkv";
    let finalized_payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 229) as u8).collect();
    let TwoSetPar2Fixture {
        volumes,
        par2_bytes,
        lost,
        ..
    } = two_set_par2_fixture(
        finalized_member,
        live_member,
        &finalized_payload,
        &live_payload,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41092);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = direct_job_with_one_finalized_neighbour(
            &mut pipeline,
            job_id,
            &volumes,
            &par2_bytes,
            lost,
        )
        .await;
        assert!(
            direct_envelopes_left(&working_dir) > 0,
            "non-vacuity: the job must die holding retained envelopes"
        );
        working_dir
    };

    // The restart. Same working directory, same spec, fresh pipeline: exactly
    // what a killed process leaves behind.
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, _) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
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

    assert_eq!(
        direct_envelopes_left(&working_dir),
        0,
        "a retained envelope is claimed by nothing after a restart — its set's \
         checkpoint row was retired at finalization — so the orphan sweep must \
         delete it rather than leave it for the job's whole second life"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| set.retained_volumes().is_none()),
        "and no restored set may believe it is holding an image: retention is \
         never restored, because there is nothing on disk left to serve"
    );
}

#[tokio::test]
async fn an_encrypted_finalized_set_keeps_a_retained_image_that_reads_back_as_posted() {
    // The retention path, over cipher. Encrypted sets were once refused
    // a retained image outright: its destinations hold **plaintext** while PAR2
    // describes the posted **cipher**, so an image assembled from the committed
    // members answered every read that crossed a member with the wrong bytes.
    //
    // The re-encrypting overlay is what makes that image honest, and a commit is
    // a rename, so the committed member is byte-for-byte the partial the live
    // image read. The assertion is therefore not "it retained something" but
    // that the retained image reads back **as the source volumes were posted** —
    // which is the only property the neighbour's repair depends on.
    let encrypted_member = "Silver.Horizon.S02E18.mkv";
    let live_member = "Amber.Trail.S01E03.mkv";
    let encrypted_payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 233) as u8).collect();
    let encrypted_set = encrypted_store_set(
        encrypted_member,
        &encrypted_payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let live_set = renamed_set(
        "amber.trail",
        recovery_record_store_set(live_member, &live_payload, 3, 256),
    );
    let volumes: Vec<(String, Vec<u8>)> = encrypted_set
        .iter()
        .chain(live_set.iter())
        .cloned()
        .collect();
    let par2_bytes = repairable_par2_index(&volumes, 16);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41093);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (mut spec, _index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The encrypted set arrives whole, so it is the one that finalizes. Its
    // neighbour gets each volume's first article only: enough to open an
    // envelope of its own — which the last assertion reads as a control — and
    // far from enough to finalize alongside.
    for (file_index, segment_number) in in_order_arrivals(encrypted_set.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    for file_index in encrypted_set.len() as u32..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 0).await;
    }
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "non-vacuity: the set under test must really be decrypting at write time"
    );

    pipeline.par2_verified.insert(job_id);
    pipeline.finalize_ready_direct_sets(job_id).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let encrypted_index = pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .position(|set| set.router.routes_encrypted())
        .unwrap_or_else(|| panic!("the encrypted set must still be there; got {sets}"));
    assert!(
        pipeline
            .direct_store
            .set(job_id, encrypted_index)
            .is_some_and(|set| set.is_finalized()),
        "non-vacuity: the encrypted set must have finalized beside a live \
         neighbour, which is the only state the refusal has anything to do in; \
         got {sets}"
    );
    assert_eq!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| !set.is_finalized() && !set.is_demoted())
            .count(),
        1,
        "and its neighbour must still be live, or the refusal is indistinguishable \
         from the ordinary no-neighbour delete; got {sets}"
    );

    let retained = pipeline
        .direct_store
        .set(job_id, encrypted_index)
        .and_then(|set| set.retained_volumes().map(<[_]>::to_vec))
        .unwrap_or_else(|| {
            panic!("an encrypted set the overlay can serve must keep its image; got {sets}")
        });
    let envelopes = pipeline
        .direct_store
        .set(job_id, encrypted_index)
        .expect("the encrypted set is there")
        .plan()
        .envelope_paths();
    assert!(
        envelopes.iter().any(|envelope| envelope.exists()),
        "a retained image reads through the envelopes, so they must outlive \
         finalization with it"
    );
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "non-vacuity for that check"
    );

    // The property the whole thing exists for: the image answers in **posted**
    // space. Reading it back must reproduce the encrypted source volumes byte
    // for byte, even though the bytes it is assembled from are the decrypted
    // member at its committed destination plus the envelopes.
    let provider = crate::pipeline::direct_store::provider::HybridVolumeProvider::new(retained);
    for (ordinal, (_, posted)) in encrypted_set.iter().enumerate() {
        let mut reader = provider
            .open(ordinal as u32)
            .expect("every retained volume is registered");
        let mut read_back = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
        assert_eq!(
            &read_back, posted,
            "retained encrypted volume {ordinal} must read back as it was posted"
        );
    }
    assert!(
        provider.cipher_counters().reencrypted_bytes() > 0,
        "non-vacuity: the read above must really have gone through the overlay \
         rather than past an empty cipher map"
    );
}

/// [`run_repairable_par2_gate`] with an article that never arrives.
///
/// A **lost article** is the only shape member-payload damage can reach PAR2
/// in. Corrupted member bytes are caught far earlier and far more cheaply by
/// direct-store's own gates — the per-part packed CRC32 at part completion, the
/// whole-member CRC32 at member completion — which demote the set during the
/// download, before a PAR2 index has even been parsed. What those gates cannot
/// do is *manufacture* bytes that never came, so a hole in a member's packed
/// range survives to the PAR2 pass, and repairing it is exactly what repair is
/// for.
async fn run_lost_article_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    lost: (u32, u32),
    password: Option<&str>,
) -> RepairGateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let (mut spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", volumes, par2_bytes);
    spec.password = password.map(str::to_owned);
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
            .or_else(|| staging_member(&complete_dir, member_name))
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
        None,
    )
    .await;
    let direct = run_lost_article_gate(
        DirectStoreGate::Enabled,
        JobId(41072),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        None,
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

#[tokio::test]
async fn a_lost_article_inside_an_encrypted_member_demotes_instead_of_repairing() {
    // The encrypted twin of `a_lost_article_inside_a_member_repairs_and_
    // reconfirms_the_volume`, whose absence let this hide for so long. It pins
    // a **known limit**, not the behaviour anyone wants.
    //
    // The plaintext twin above repairs the hole in place and never demotes. The
    // same shape over an encrypted set cannot, and it fails over roughly eight
    // bytes: the cipher block straddling the hole's edge is held, because its
    // other half is in the article that never came, so the volume's covered run
    // stops just short of an article boundary. `CrcRuns::compose` composes a
    // reference only for a range that starts *and ends* on one — deliberately,
    // since this sweep reads an overlay of sparse files where a source answering
    // with zeros yields bytes that look like data and pass nothing — so the run
    // is `UnverifiableRun`, materialization is refused, and the repair with it.
    //
    // The user still gets correct bytes: the set demotes, refetches, and the
    // conventional path extracts. What it costs is the whole set, for a hole
    // PAR2 could have filled. The plan's Risks section records the fix (let
    // reconstruction take those held bytes as posted *cipher* straight from
    // staging — materialization rebuilds posted bytes, not plaintext, so they
    // never needed decrypting). **When that lands this test should fail**, and
    // the right response is to rewrite it against the plaintext twin's
    // assertions rather than to relax it.
    let member_name = "Silver.Horizon.S01E25.mkv";
    let password = "moonlit-harbour";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 223) as u8).collect();
    let volumes = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        password,
        Some(password),
        true,
        256,
    );
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41081),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        Some(password),
    )
    .await;
    let direct = run_lost_article_gate(
        DirectStoreGate::Enabled,
        JobId(41082),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        Some(password),
    )
    .await;

    // Non-vacuity: the conventional path really does repair this hole, so the
    // direct path's failure is about the direct path and not the fixture.
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the lost article; status={:?}",
        conventional.status
    );
    // The limit, stated as behaviour.
    assert!(
        direct.sets.contains("Demoted"),
        "an encrypted set is expected to demote on a lost article until the \
         reconstruction fix lands; if this now repairs in place, rewrite this \
         test against the plaintext twin. sets = {}",
        direct.sets
    );
    // What this test deliberately does **not** assert, because the harness
    // cannot establish it either way: that the demoted set still delivers the
    // member. It does not, here — `direct.member` is `None` against the
    // conventional path's full payload — but the lost article is permanently
    // gone and there is no server, so the refetch a demotion schedules can never
    // be answered. The conventional run recovers only because it wrote real
    // volume files as articles arrived, giving PAR2 something on disk to repair.
    //
    // So this either costs a refetch (the reviewer's reading) or costs the
    // member outright (what the harness shows), and telling those apart needs a
    // fixture that models a refetch which can actually succeed. **That is the
    // open question**, and it is the reason the plan's Risks entry now carries
    // the real mechanism instead of the CBC one.
    assert!(
        direct.member.is_none() || direct.member.as_deref() == conventional.member.as_deref(),
        "a demoted encrypted set must either deliver the conventional bytes or \
         nothing at all — never different bytes; sets = {}",
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
    // The wave-2 review note, priced. The middle article of the middle volume
    // is lost, so the volume has an interior hole with healthy bytes on both
    // sides. Before repair landed the verifier's sequential sweep stopped at
    // that hole and called every slice after it damaged; a repair sized from
    // that count spends a recovery block per slice, and the ones past the hole
    // are blocks spent rebuilding bytes that were never broken. Enough of them
    // and a repairable set reads as unrepairable.
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
// The config surface, and sparse marking
// ---------------------------------------------------------------------------

/// Every other test here reaches for `set_gate`. This one comes through
/// configuration, which is what the operator surface exposes: the
/// `[direct_store]` table turns routing on, and turning it **off** at startup
/// makes a restart ignore and sweep the mid-flight direct state and redownload
/// the job conventionally.
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

    // First run: config on. The job routes, so partials, envelopes and a
    // coverage row exist — the non-vacuity the sweep assertions below depend
    // on.
    let working_dir = {
        let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
            &temp_dir,
            DirectStoreOverrides {
                enabled: Some(true),
                holds_scratch_ceiling_bytes: None,
            },
        )
        .await;
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

    let partial = direct_partial(&temp_dir, JobId(41090), member_name);
    let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
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

    // Second run: config off. Same working directory, same spec, a fresh
    // pipeline.
    let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
        &temp_dir,
        DirectStoreOverrides {
            enabled: Some(false),
            holds_scratch_ceiling_bytes: None,
        },
    )
    .await;
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

/// A destination that cannot be marked sparse demotes the set, and the refusal
/// happens before the file holds a hole — so nothing it created is left on
/// disk.
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
        !direct_partial(&temp_dir, JobId(41091), member_name).exists(),
        "the refused destination must not be left behind"
    );
    assert!(
        !working_dir
            .join("silver.horizon.f0.vol00000.envelope")
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
// Encrypted direct-store
//
// The spine is the same differential established earlier, with one input added:
// the identical job is run with routing on and off *with the same password*,
// and the outputs must be byte-identical. With routing on, no source volume may
// ever appear on disk — which for an encrypted set also means the ciphertext
// never does.
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
/// one shape the cipher-block holds exist for.
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
    encrypted_store_set_with_recovery(
        member_name,
        payload,
        volume_count,
        data_password,
        check_for,
        keyed_checksum,
        0,
    )
}

/// [`encrypted_store_set`] with a recovery record after each volume's payload.
///
/// The RR is what makes PAR2 the *only* layer that can see a damaged byte: it
/// belongs to no member, so neither the per-part packed CRC32 over cipher nor
/// the keyed whole-member fold over plaintext covers it, and it is a service
/// block's data rather than a header, so the walk still parses and the volume
/// still confirms. Every byte of it is envelope, posted in the clear, and
/// routing carries it through untouched — which is exactly why a damaged one
/// survives to the pass.
#[allow(clippy::too_many_arguments)]
fn encrypted_store_set_with_recovery(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    check_for: Option<&str>,
    keyed_checksum: bool,
    rr_bytes: usize,
) -> Vec<(String, Vec<u8>)> {
    let material =
        unrar_rs::derive_rar5_material(data_password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable");
    let key = material.key;
    let hash_key = material.hash_key;
    let psw_check = check_for.map(|password| {
        unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable")
            .psw_check
    });

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut padded = payload.to_vec();
    padded.resize(cipher_len, 0);
    let cipher = unrar_rs::test_support::encrypt_aes256_cbc(&key, &TEST_CRYPT_IV, &padded);

    let member_crc = checksum::crc32(payload);
    let member_crc = if keyed_checksum {
        unrar_rs::convert_crc32_to_mac(member_crc, &hash_key)
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
            if rr_bytes > 0 {
                bytes.extend_from_slice(&build_test_rar_service_header("RR", rr_bytes as u64));
                bytes.extend((0..rr_bytes).map(|index| ((index * 7 + volume * 13) % 256) as u8));
            }
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
    let partial_seen = any_direct_partial(&payload_root(&temp_dir, job_id));
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

    // Non-vacuity, and the keyed-fold claim itself: the identical fixture with
    // the right password verifies through the same keyed fold and completes.
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

    // Every volume's header article first, ascending, then the payload halves in
    // reverse. The headers alone complete the chain, so the drain runs for every
    // volume while each one's *predecessor* bytes are still outstanding: volume
    // n's part starts at a non-block-aligned cipher offset, so its first block's
    // 16 preceding bytes live in the tail of volume n-1 — which has not arrived.
    // Every part boundary is exercised in both directions, and the reverse
    // payload order means the holds are released from the far end back.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 0)).collect();
    arrivals.extend((0..volumes.len() as u32).rev().map(|index| (index, 1)));

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

    // The direct side is driven inline rather than through the gate helper, for
    // one assertion the helper cannot make: byte-identical output proves the
    // drain produced the right bytes, but it cannot tell a set that *held* a
    // straddling block from one whose arrival order never made it hold. The
    // counter is that difference, and without it this test would pass just as
    // happily against a build that had deleted the edge-hold path.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43042);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    spec.password = Some("moonlit-harbour".to_string());
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

    let blocks_held = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the encrypted set must still be routing")
        .router
        .blocks_held();
    assert!(
        blocks_held > 0,
        "this arrival order must have made the drain hold a cipher block for a \
         predecessor it did not have yet; it held {blocks_held}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    let status = job_status_for_assert(&pipeline, job_id);

    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a held straddling block must drain to the same plaintext once its other half lands"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "holding a cipher block must not fall back to writing the volume"
    );
    assert_eq!(
        (member, member_location, status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "out-of-order encrypted arrival must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_forged_password_check_admits_and_the_keyed_member_gate_catches_it_anyway() {
    // The forgeable-check risk, as a test. The RAR5 password check is 8
    // unauthenticated bytes a writer chooses, so a hostile archive can carry the
    // check for a password that does **not** decrypt its data and have admission
    // report `Verified`. That is why the check is an admission *test* and never a
    // reason to skip the keyed member gate: the gate is the authority.
    let member_name = "Silver.Horizon.S02E14.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    // Data encrypted with one password, the header's check forged for another —
    // and the job holds the forged one.
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("wrong-key"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(43131), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        !caught.shape.contains("EncryptedMemberRefused"),
        "the forged check must have *passed* admission — otherwise this test proves \
         nothing about the gate behind it, got {}",
        caught.shape
    );
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "the keyed whole-member fold is the authority a forged check cannot reach, got {}",
        caught.shape
    );
    // Deliberately no volume-file assertion, for the same reason
    // `a_wrong_password_with_no_check_routes_until_the_keyed_member_gate_catches_it`
    // has none: this gate fires on the *last* article, when the member's
    // plaintext first composes, so the handover has no further articles to ride
    // and the materialization is the demotion machinery's own.
    assert!(
        !caught.partial_seen,
        "the refused member's destination must not be left on disk for the \
         conventional extractor to find"
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
async fn a_password_that_arrives_before_the_first_article_still_admits_the_set() {
    // Named for what it actually proves. weaver *does* support setting a
    // password after add (`setJobPassword`, and the NZBGet facade's
    // `*Unpack:Password`), both of which mutate the live job spec, and the
    // direct sets are built once per job and memoized, so the seam re-reads the
    // spec while any set is still willing to take one.
    //
    // The window is **pre-first-article**, not "any time during the download":
    // admission runs from the first successful header parse, and its
    // `NoPassword` refusal is a demotion. A password arriving after that finds
    // the set already on the conventional path, which asks the job's whole
    // candidate list anyway. Waiting for one instead would mean holding every
    // arriving byte for a set that will most likely never get a password, and
    // then throwing all of it away on a scratch-ceiling breach.
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
async fn a_password_corrected_before_the_first_article_admits_with_the_correction() {
    // The dead branch, end to end. `KeyRing::set_password` has always handled a
    // password *changing* while nothing is admitted — but the seam that calls
    // it stopped asking the moment any password was held, so a job added with
    // the wrong one and corrected before its first header parsed derived keys
    // from the stale one, admitted on the header's check, and failed the keyed
    // member gate a whole download later.
    let member_name = "Silver.Horizon.S02E16.mkv";
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
    let job_id = JobId(43141);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    // Added with a typo.
    spec.password = Some("moonlit-harbor".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The sets are built here, holding the wrong password — which is exactly the
    // state the old window treated as settled.
    assert!(
        pipeline
            .direct_route_target(crate::jobs::ids::NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_some(),
        "the set should be admitted and still routing before anything has parsed"
    );

    // The correction, through the same live-spec write both API surfaces do.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the correction must be the password admission derives from, got {shape}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a password corrected before the first article must produce the member"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the corrected set must still route, not materialize"
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
// The machinery encryption changes nothing about, asserted rather than
// assumed. Each of these is the encrypted twin of a plaintext guarantee.
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
    let job_id = JobId(43101);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Payload before headers, so the holds machinery carries cipher bytes the
    // layout cannot place yet — the same hold a plaintext set uses, over cipher.
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

    // Suppression: an encrypted set's source volumes are direct volumes, so no
    // legacy floor, completed-file row or archive re-probe may be written.
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
    // time, so these are the encrypted twins of the plaintext guarantees rather
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
async fn a_par2_bearing_encrypted_job_routes_direct_and_completes_byte_identically() {
    // The headline differential, and the one the overlay exists for. An earlier
    // shape refused this job at admission
    // (`EncryptedMemberRefused(Par2Declared)`) because an encrypted set's
    // destinations hold plaintext while PAR2 describes the posted cipher — and
    // nearly every encrypted release carries PAR2, so that refusal reached
    // almost every set the feature was built for.
    //
    // The re-encrypting overlay retires it: the pass reads the set's source
    // volumes virtually, the overlay turns the member ranges back into what was
    // posted, and the verdict is the one a physical volume would have produced.
    // Held to exactly the standard the plaintext par2-bearing differential is:
    // same bytes, same place, same status as the gate-off run with the same
    // password, and not one source volume file at any point.
    let member_name = "Silver.Horizon.S02E12.mkv";
    // 2400 is a multiple of 16, so the *other* differential below carries the
    // tail padding; this one pins the block-aligned shape.
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(43110),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(43111),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing job must admit its encrypted sets now that the verifier \
         can read them as they were posted"
    );
    assert!(
        !direct.demotions.contains("Demoted"),
        "nothing here may demote the set, got {}",
        direct.demotions
    );
    assert!(
        conventional.volume_file_seen,
        "the gate-off reference must have written the encrypted source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a par2-bearing encrypted direct job must never create a source volume \
         file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must decrypt the member with the job's password"
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
        "an encrypted par2-bearing job routed plaintext-once must be byte-identical \
         to the gate-off run with the same password; sets = {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set should have finalized once verification cleared it, got {}",
        direct.demotions
    );
    // The point, unchanged by encryption: verification finishes with the
    // download, and it does so entirely against virtual volumes — the pass
    // reads the set through the overlay, which answers in posted space.
    assert!(
        direct.verdict_reached,
        "the clean encrypted set must have reached a PAR2 verdict; \
         authoritative={}",
        direct.authoritative_verify_calls
    );
}

#[tokio::test]
async fn a_recovery_set_the_spec_never_declared_no_longer_demotes_an_encrypted_set() {
    // The belt, and what the overlay leaves of it. A PAR2 file the job's spec
    // did **not** classify as one — a deobfuscated or renamed file — can make a
    // recovery set real mid-job, long after an encrypted set started routing.
    // An earlier shape had to demote such a set before the authoritative pass,
    // because the pass would otherwise have read plaintext where cipher
    // belongs; the whole set's volumes then came back off the wire.
    //
    // With the overlay the pass reads posted bytes, so the belt no longer fires
    // on "encrypted" at all. It fires only on
    // `posted_bytes_unavailable`, and this set can serve every byte it routed —
    // so the assertion is that the set is **still direct** and that its virtual
    // volumes really do answer as posted.
    //
    // Modelled by stripping the PAR2 role from the *spec* after the job is
    // inserted: the assembly keeps its own copy, so the index still parses
    // through the production path, while `job_spec_has_par2_file` answers no.
    // The last article is held back so the set is still live when the recovery
    // set lands, which is the only state the guard has anything to do in.
    let member_name = "Silver.Horizon.S02E15.mkv";
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
    let job_id = JobId(43112);
    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .files[index_file_index as usize]
        .role = weaver_model::files::FileRole::Unknown;

    let held_back = (volumes.len() as u32 - 1, 1u32);
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == held_back {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    // Non-vacuity: it really did route the encrypted set before the recovery set
    // turned up, so what the guard decides below is about a live encrypted set.
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
    assert!(
        pipeline.par2_set(job_id).is_some(),
        "the index must have parsed through the production path, or the guard has no \
         recovery set to react to and the test proves nothing"
    );

    assert!(
        !pipeline.demote_unbindable_direct_sets(job_id).await,
        "a late recovery set must no longer take an encrypted set out of direct mode"
    );
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the set must still be routing after the guard has run, got {shape}"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and nothing may have been materialized: the set never left direct mode"
    );

    // The reason it may stay: every volume it has bytes for answers as posted.
    // The last one is deliberately short an article, so this also pins that a
    // half-arrived volume reads its covered prefix rather than refusing whole.
    for (file_index, (_, posted)) in volumes.iter().enumerate().take(volumes.len() - 1) {
        let file_index = file_index as u32;
        let (volume_index, _, provider) = pipeline
            .direct_virtual_volume(NzbFileId { job_id, file_index })
            .unwrap_or_else(|| panic!("volume {file_index} must answer through the overlay"));
        let mut reader = provider.open(volume_index).expect("registered");
        let mut read_back = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
        assert_eq!(
            &read_back, posted,
            "encrypted direct volume {file_index} must read back as it was posted"
        );
    }
    let _ = &complete_dir;
}

#[tokio::test]
async fn par2_damage_in_an_encrypted_sets_envelope_repairs_while_the_set_stays_direct() {
    // The first transition, over cipher. The damaged byte is in a recovery
    // record's data area — outside the member's packed range, so neither the
    // per-part packed CRC32 over cipher nor the keyed whole-member fold over
    // plaintext covers it, and inside a service block's data rather than a
    // header, so the walk still parses and the volume still confirms. PAR2 is
    // the only layer that can see it.
    //
    // Everything the repair then does goes through the overlay: the damaged
    // volume is **materialized** out of the set's own bytes, which for an
    // encrypted set means re-encrypting every member range it covers, and the
    // repaired spans re-enter the router with `replace` set, which is the path
    // The cache-invalidation guards. Held to the same standard as the plaintext
    // version: the gate-off run with the same password.
    let member_name = "Silver.Horizon.S02E21.mkv";
    // Not a multiple of 16, so the member carries tail padding and the
    // materialization has to re-encrypt a final block out of it.
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
        rr_bytes,
    );
    // The PAR2 set describes the *clean* volumes and carries enough recovery to
    // rebuild the damaged slice; the job downloads a damaged volume.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate_at(
        DirectStoreGate::Disabled,
        JobId(43121),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_repairable_par2_gate_at(
        DirectStoreGate::Enabled,
        JobId(43122),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and decrypt the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        (direct.member.as_deref(), &direct.status),
        (conventional.member.as_deref(), &conventional.status),
        "a repaired encrypted direct set must produce the gate-off output, with \
         the same status; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "repair-while-direct materializes only under a scratch name, never the \
         volume's own; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "the materialized copies must be deleted whether the repair succeeded or fell back"
    );
    assert_eq!(
        direct.materialized, 1,
        "only the damaged volume may be materialized, and it must have been; \
         sets = {}",
        direct.sets
    );
    assert!(
        direct.finalized > 0,
        "the set must have stayed direct and committed its members from its own \
         partials; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "nothing here may demote the set, got {}",
        direct.sets
    );
}

#[tokio::test]
async fn par2_damage_in_a_split_encrypted_members_first_article_repairs_while_the_set_stays_direct()
{
    // The drain-order finding, and the coverage gap that hid it: every other
    // repair test in this file damages a *recovery record* in a volume's
    // **last** article, so the rewrite it provokes never reaches a member
    // extent's first cipher block. Here the damaged volume is posted as a
    // single article, so the rewrite — widened to whole articles — starts at
    // physical zero and the whole of volume 1's member extent is re-routed,
    // first block included.
    //
    // That block straddles the volume boundary: part of it was posted in volume
    // 0, and its CBC predecessor lies wholly in volume 0. Both were routed and
    // dropped from staging long before the repair, and no checkpoint survives at
    // the extent's low edge once the two volumes' decrypted runs coalesce — so
    // `cipher_edge_reads` is the only thing that can put them back. It used to
    // ask for the straddling bytes alone, which left the block undecryptable,
    // the span held, and the whole set demoted under `RepairRerouteFailed` into
    // a full refetch of every article.
    //
    // Held to the same standard as the other repair differentials: the gate-off
    // run with the same password, byte for byte.
    let member_name = "Silver.Horizon.S02E24.mkv";
    // Not a multiple of 16, so the member also carries tail padding.
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 223) as u8).collect();
    let rr_bytes = 512;
    let clean = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
        rr_bytes,
    );
    // The RR keeps PAR2 the only layer that can see the damage: it belongs to no
    // member, so no packed or whole-member checksum covers it. What makes this
    // test different is not where the damage is but how wide the *rewrite* is.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate_with_articles(
        DirectStoreGate::Disabled,
        JobId(43151),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
        1,
    )
    .await;
    let direct = run_repairable_par2_gate_with_articles(
        DirectStoreGate::Enabled,
        JobId(43152),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
        1,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and decrypt the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        (direct.member.as_deref(), &direct.status),
        (conventional.member.as_deref(), &conventional.status),
        "a repair reaching a split encrypted member's first block must produce \
         the gate-off output, with the same status; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and it must not demote — a set that reroutes its own repair never \
         refetches an article; got {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "repair-while-direct materializes only under a scratch name, never the \
         volume's own; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "only the damaged volume may be materialized, and it must have been; \
         sets = {}",
        direct.sets
    );
    assert_eq!(direct.repair_scratch_left, 0);
    assert!(
        direct.finalized > 0,
        "the set must have stayed direct and committed its members from its own \
         partials; sets = {}",
        direct.sets
    );
}

#[tokio::test]
async fn an_encrypted_set_that_demotes_rebuilds_byte_exact_posted_volumes() {
    // The *other* transition, over cipher. An earlier shape refused
    // reconstruction for an encrypted set outright
    // (`ReconstructionFailure::EncryptedPostedBytes`) and refetched every
    // article instead, because the member partials hold plaintext where the
    // volume holds cipher.
    //
    // The overlay makes the sweep read posted bytes, so a demoting encrypted set
    // materializes its volumes out of its own routed bytes like any other — and
    // the standard is byte-exactness against what was posted, not merely "a file
    // appeared", because reconstruction publishes a floor over what it writes
    // and nothing downstream ever re-checks those bytes.
    //
    // The demotion is forced by a reason that has nothing to do with encryption
    // — a scratch failure — so what is under test is the sweep and only the
    // sweep. What keeps the set *live* while every article is in is the job's
    // PAR2 file: finalization waits for a verdict, and the index is never
    // posted, so the set sits in the wait a demotion can still reach. (A
    // finalized set can never be demoted, and one that has committed its members
    // has nothing left to reconstruct from.)
    //
    // Every article, deliberately. An encrypted member's last cipher block in
    // volume *n* straddles into volume *n+1*, so a set missing any article holds
    // the ≤15 bytes below that boundary — and the covered run then stops
    // mid-article with nothing in the yEnc composition to vouch for it, which is
    // `UnverifiableRun` and a refusal to reconstruct at all. That is correct,
    // fail-closed behaviour; it is simply not what this test is about.
    let member_name = "Silver.Horizon.S02E22.mkv";
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 197) as u8).collect();
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
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43131);
    let (mut spec, _index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Dequeued as they are delivered, so the queue at the end says what the
    // demotion put *back* rather than what the harness never took out.
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "non-vacuity: the set must have routed encrypted before it is demoted"
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsScratchFailed)
        .await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted"),
        "the set must have demoted, got {shape}"
    );

    // The whole point: real files, byte-exact against what was *posted* rather
    // than against what the destinations hold.
    for (filename, posted) in &volumes {
        let rebuilt = std::fs::read(working_dir.join(filename))
            .unwrap_or_else(|error| panic!("{filename} must have been materialized: {error}"));
        assert_eq!(
            &rebuilt, posted,
            "{filename} must be rebuilt byte-exactly as it was posted, not as it \
             was decrypted"
        );
    }
    // And not one volume article came back off the wire — That refusal
    // refetched every one of them, which is exactly what the sweep exists to
    // avoid. The PAR2 index is still outstanding because it was never posted.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued
            .iter()
            .all(|(file_index, _)| *file_index as usize >= volumes.len()),
        "an encrypted set that reconstructs must not also refetch its volumes, \
         got {queued:?}"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_set_restarted_mid_download_verifies_and_completes_byte_identically()
 {
    // The restart differential, and the shape the write side could not reach: a
    // par2-bearing job was refused encrypted admission outright, so nothing
    // encrypted ever survived a restart *and* faced a verifier.
    //
    // Three things have to hold together here. The floors have to be honoured
    // (nothing below them comes back off the wire); the keyed member gate has to
    // re-arm by re-reading the pre-restart **plaintext** from disk; and the
    // overlay has to serve **posted** bytes over coverage this process never
    // wrote — which means the crypt facts, the cipher checkpoints and the
    // retained tail padding all came back through snapshot schema v4.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S02E23.mkv";
    // Not a multiple of 16, so the retained tail padding is part of what the
    // snapshot has to carry across the restart.
    let payload: Vec<u8> = (0..8005u32).map(|index| (index % 251) as u8).collect();
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
    let job_id = JobId(43141);

    // Volume 0 whole, volume 1 half: one file the restore skips entirely and one
    // it skips only part of, with a part boundary that is not 16-aligned — so
    // the resumed run decrypts at a coverage frontier whose predecessor cipher
    // block died with the process that wrote it.
    let (working_dir, index_file_index) = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
        let index_file_index = append_par2_index(&mut spec, &par2_bytes);
        spec.password = Some("moonlit-harbour".to_string());
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)] {
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
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        (working_dir, index_file_index)
    };

    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    append_par2_index(&mut spec, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
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

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored par2-bearing encrypted job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a par2-bearing encrypted set must survive a restart now that the pass \
         can read it as posted"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be \
         refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(1, 0)),
        "volume 1's checkpointed articles must not be refetched, got {queued:?}"
    );

    // The restored set answers in posted space before a single new article
    // arrives — which is only possible if the snapshot carried the crypt facts,
    // the cipher checkpoints and the tail padding across the restart.
    let (volume_index, _, provider) = pipeline
        .direct_virtual_volume(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("the restored volume must answer through the overlay");
    let mut reader = provider.open(volume_index).expect("registered");
    let mut read_back = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
    assert_eq!(
        read_back, volumes[0].1,
        "a restored encrypted volume must read back exactly as it was posted"
    );

    for (file_index, segment_number) in queued.clone() {
        if file_index == index_file_index {
            continue;
        }
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
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "the keyed member gate must have re-read the pre-restart plaintext before \
         finalizing"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a restarted par2-bearing encrypted set must still produce the member"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and it must never have materialized a source volume"
    );
}

// ---------------------------------------------------------------------------
// RAR4/RAR3 file encryption
//
// The same three cipher-hold, keyed-fold and crypt-restore mechanisms over the
// other format. Nothing in the router, the holds, the gates, the overlay or the
// snapshot is RAR5-specific; the only differences the format brings are the
// cipher width (AES-128), where the key and IV come from (both out of the KDF,
// over an 8-byte per-file salt the header states), and the two things RAR4 does
// not have — a password-check value, and a tweaked-checksum flag. Those two
// absences are the subject of the tests below as much as the presence of the
// cipher is.
// ---------------------------------------------------------------------------

/// The corpus-wide RAR4 fixture salt. RAR salts each *file* rather than the
/// archive, which is why the KDF tuple is per member here where RAR5's is per
/// archive.
const TEST_RAR4_SALT: [u8; 8] = [0x9B; 8];

/// A RAR4 end-of-archive header that **states its volume number**, which
/// [`build_test_rar4_end_header`] does not.
///
/// RAR4 has no per-volume number anywhere else — RAR5 carries one in the main
/// header, RAR4 carries it in `ENDARC` behind the `VOLUME_NUMBER` flag — and
/// `unrar-rs` reads `RarVolumeFacts::volume_number` from exactly there. The
/// **conventional** path keys its whole per-set volume map by that number
/// (`persist_rar_volume_facts`), so a set whose volumes all report volume 0
/// collapses into one entry and never assembles. Direct-store does not care:
/// it numbers volumes by NZB file index, which is why the plaintext RAR4
/// fixture gets away without this. A *differential* needs both halves to work.
fn build_test_rar4_end_header_numbered(more_volumes: bool, volume: u16) -> Vec<u8> {
    let mut flags: u16 = 0x0004; // VOLUME_NUMBER
    if more_volumes {
        flags |= 0x0001; // NEXT_VOLUME
    }
    build_test_rar4_block(0x7b, flags, &volume.to_le_bytes())
}

/// A stored, encrypted RAR4 file header: [`build_test_rar4_file_header`] plus
/// the `ENCRYPTED`/`SALT` flags and the 8-byte salt the format appends after the
/// filename.
///
/// `unpack_version` 29 is what selects "RAR 3.0" encryption — AES-128-CBC. The
/// three older values select ciphers `unrar-rs` refuses to classify as an
/// encrypted store at all, which is asserted in the library rather than here.
fn build_test_rar4_encrypted_file_header(
    filename: &str,
    split_flags: u16,
    packed_size: u32,
    unpacked_size: u32,
    data_crc: u32,
    salt: Option<[u8; 8]>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packed_size.to_le_bytes());
    body.extend_from_slice(&unpacked_size.to_le_bytes());
    body.push(3); // host OS: Unix
    body.extend_from_slice(&data_crc.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // mtime
    body.push(29); // unpack version: RAR 3.0, i.e. AES-128
    body.push(0x30); // method: store
    body.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    body.extend_from_slice(&0o644u32.to_le_bytes());
    body.extend_from_slice(filename.as_bytes());
    let mut flags = 0x8000 | 0x0004 | split_flags;
    if let Some(salt) = salt {
        flags |= 0x0400;
        body.extend_from_slice(&salt);
    }
    build_test_rar4_block(0x74, flags, &body)
}

/// The RAR4 twin of [`encrypted_store_set`]: one `-m0 -p` stored member split
/// across `volume_count` volumes.
///
/// Mirrors what `rar a -ma4 -m0 -p<password> -v<size>` writes, which is the
/// recipe `rarpar`'s `tests/fixtures/generate_encrypted.sh` records for
/// `rar4_enc_mv_store`: the member's whole plaintext as **one** AES-128-CBC
/// stream running unbroken across the volume boundaries — a property held
/// against that real archive in rarpar's
/// `a_rar4_encrypted_chain_is_one_cbc_stream_keyed_by_its_file_salt` — with
/// `align16(unpacked_size)` cipher bytes in total, the salt repeated on every
/// part's header, plain packed CRC32s over cipher on the non-final parts and the
/// whole-member CRC32 on the last.
///
/// Two things a RAR5 fixture can carry and this one cannot, by construction:
///
/// - **no password-check value.** RAR4 has no such field, so no `check_for`
///   parameter exists here and admission can never refute a password.
/// - **no keyed checksum.** RAR4 has no hash-MAC flag, so the whole-member
///   CRC32 below is the bare plaintext CRC32 and `convert_crc32_to_mac` is not
///   applied to it. That is that finding stated as a fixture.
fn encrypted_rar4_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    data_password: &str,
    salt: Option<[u8; 8]>,
) -> Vec<(String, Vec<u8>)> {
    let (key, iv) = unrar_rs::rar4_derive_key(data_password, salt.as_ref());

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut cipher = payload.to_vec();
    cipher.resize(cipher_len, 0);
    // The public range API, not a `#[doc(hidden)]` test helper: the posted bytes
    // a fixture claims to have been posted should come from the same surface the
    // overlay re-derives them with.
    unrar_rs::encrypt_cipher_range_rar4(&key, &iv, &mut cipher)
        .expect("the padded payload is block-aligned");

    let member_crc = checksum::crc32(payload);
    let mut offset = 0usize;
    misaligned_parts(cipher_len, volume_count)
        .into_iter()
        .enumerate()
        .map(|(volume, part_len)| {
            let part = &cipher[offset..offset + part_len];
            offset += part_len;
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
            bytes.extend_from_slice(&build_test_rar4_encrypted_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
                salt,
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header_numbered(
                !is_last,
                volume as u16,
            ));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

#[tokio::test]
async fn an_encrypted_rar4_store_set_routes_plaintext_and_matches_the_conventional_extractor() {
    // The differential spine, over RAR4. 3000 is not a multiple of 16, so the
    // member carries 8 bytes of tail padding — the AES-128 final block decrypts
    // to plaintext that runs past the member's declared end, and none of it may
    // reach the destination.
    let member_name = "Silver.Horizon.S03E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(44001),
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
        JobId(44002),
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
        "direct routing must never create a source volume file, RAR4 or RAR5"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should decrypt the RAR4 member with the job's password"
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
        "an encrypted RAR4 set routed plaintext-once must be byte-identical to the \
         conventional extractor with the same password"
    );
}

#[tokio::test]
async fn a_saltless_rar4_header_keys_off_the_password_alone_and_still_routes() {
    // RAR3 archives written without a salt derive the key from the password
    // alone. That is a complete description of the member's keying, not a
    // missing record, and the router must treat it as one rather than refusing.
    let member_name = "Silver.Horizon.S03E02.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_rar4_store_set(member_name, &payload, 3, "moonlit-harbour", None);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(44011),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "a saltless RAR4 member must route and verify like a salted one"
    );
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn a_wrong_password_on_a_rar4_set_is_caught_by_the_member_gate_and_nothing_earlier() {
    // The keyed-fold claim for RAR4, and the one place the format differs in a
    // way that matters: there is **no** password-check value, so
    // `WrongPassword` cannot fire at admission however wrong the password is.
    // Every RAR4 member routes provisionally and the whole-member CRC32 is the
    // only detector — a *plain* CRC32, not a keyed fold, because RAR4 has no
    // hash-MAC flag.
    let member_name = "Silver.Horizon.S03E03.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(44021), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "a wrong password on a RAR4 set must be caught by the member gate, got {}",
        caught.shape
    );
    assert!(
        !caught.shape.contains("EncryptedMemberRefused"),
        "and it must NOT be refused at admission — asserting the absence so the test \
         cannot pass by refusing early instead, which would prove nothing about the \
         gate: RAR4 has no check value to refute with. Got {}",
        caught.shape
    );

    // Non-vacuity: the identical fixture with the right password verifies
    // through the same gate and completes.
    let clean = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(44022),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        clean.member.as_deref(),
        Some(payload.as_slice()),
        "the same RAR4 set with the right password must verify and complete"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_with_no_password_demotes_instead_of_routing_ciphertext() {
    let member_name = "Silver.Horizon.S03E04.mkv";
    let payload: Vec<u8> = (0..1500u32).map(|index| (index % 181) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let outcome = encrypted_routing_outcome(JobId(44031), &volumes, &arrivals, None).await;
    assert!(
        outcome
            .shape
            .contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "an encrypted RAR4 set with no password must demote, by name, got {}",
        outcome.shape
    );
    assert!(
        !outcome.partial_seen,
        "a set with no key must not create a destination for ciphertext"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_rar4_job_routes_direct_and_completes_byte_identically() {
    // The re-encrypting overlay, over AES-128: the destinations hold plaintext
    // while PAR2 describes the posted cipher, so every block the authoritative
    // pass reads has to be re-derived through `MemberCipher::encrypt` — which
    // is now a dispatch rather than a fixed width. If that dispatch were wrong
    // the pass would report damage in a byte-perfect volume and the set would
    // demote.
    let member_name = "Silver.Horizon.S03E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(44041),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(44042),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing encrypted RAR4 job must route, got {}",
        direct.demotions
    );
    assert!(
        !direct.demotions.contains("Demoted"),
        "and it must stay direct through verification, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "no source volume may be written at any point"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the routed member must match the payload byte for byte"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a par2-bearing encrypted RAR4 set must be byte-identical to the conventional path"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_restarted_mid_download_completes_byte_identically() {
    // Crypt-state restore over RAR4. What a restart has to rebuild here is
    // *not* what it rebuilds for RAR5: the snapshot row carries the 8-byte file
    // salt and no IV, because RAR4's IV is a KDF output and persisting one
    // would put a password verifier in the database that the archive itself
    // does not have. The resumed run re-derives both key and IV from the live
    // password, and the checkpointed cipher blocks are what let it decrypt at
    // the coverage frontier rather than from the member's start.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S03E06.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(44051);
    // Volume 0 complete, volume 1 half: one file the restore can skip whole and
    // one it must skip only part of, with a part boundary that is not 16-aligned.
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
        .expect("the restored encrypted RAR4 job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that still has the password must re-admit the set, not demote it — \
         which for RAR4 means the salt in the row agreed with the rebuilt headers and \
         the IV was re-derived, got {set:?}"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, \
         got {queued:?}"
    );
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
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
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted encrypted RAR4 set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "a restarted encrypted RAR4 set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_restarted_without_its_password_demotes_by_name() {
    // The RAR4 twin of
    // `an_encrypted_set_restarted_without_its_password_demotes_by_name`, and
    // the one case RAR4 cannot borrow the RAR5 argument for.
    //
    // RAR5 can refute a wrong password at admission against the header's check
    // value. RAR4 has no check value at all, so its admission can only answer
    // one question: is there a password? A restore that supplies none must
    // therefore refuse on *absence* — never derive a key from an empty or
    // default password, which would produce a well-formed AES-128 stream and
    // route decrypted garbage until the whole-member CRC32 noticed. The refusal
    // runs before the checkpoint can seed anything, so nothing is left
    // permanently unverifiable either.
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S03E07.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(44061);
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

    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "a RAR4 set that cannot decrypt must not be left holding seeded coverage it can \
         never re-arm"
    );
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused checkpoint must skip nothing, got {queued:?}"
    );

    // And the live parse reaches the same decision under its own name, rather
    // than routing ciphertext or holding forever.
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "a restored encrypted RAR4 set with no password must demote by name, got {shape}"
    );

    // The demotion is the conventional path taking over, not a dead end: the
    // stale `.direct.partial` from the pre-restart run is swept by suffix, so
    // no plaintext prefix written under the old key is left to be mistaken for
    // this run's output.
    assert!(
        !any_direct_partial(&payload_root(&temp_dir, JobId(44061))),
        "a refused restore must sweep the partials the previous run wrote"
    );
}

// ---------------------------------------------------------------------------
// Header-encrypted (`-hp`) sets
//
// `-hp` withholds the *layout*, not the *keying facts*: RAR5's type-4 record is
// plaintext and states the salt, the KDF count and — by default — a password
// check. So a set whose password rides in the NZB can be keyed at its very first
// article, before any byte routes, and route direct like any other encrypted set.
//
// The load-bearing difference from `-p` runs through every test below: `-p`
// admits on `Unverifiable`, because a wrong key there only corrupts *data* and
// the whole-member CRC32 catches it. Here a wrong key corrupts the **header
// parse**, so the layout itself would come out of garbage — and "garbage will
// not parse" is a 2^-32 argument, not a gate. `-hp` therefore admits on
// `Verified` and on nothing else, and every other outcome is a named refusal
// back to the older floor: materialize the volume, extract conventionally.
// ---------------------------------------------------------------------------

/// The archive-level KDF tuple every `-hp` fixture here shares. `lg2 = 4` for
/// the reason [`TEST_CRYPT_KDF_LG2`] is: real archives use 2^15 and up, and
/// nothing under test reads the number.
const TEST_HP_SALT: [u8; 16] = [0x3C; 16];
const TEST_HP_KDF_LG2: u8 = 4;

/// What a fixture's type-4 record claims about its password check.
#[derive(Clone, Copy)]
enum HeaderCheck {
    /// A check the parser will validate and hand out: the flag set, eight check
    /// bytes from this password's KDF, and their real SHA-256 tag. What WinRAR
    /// writes by default, and the only shape `-hp` admission accepts.
    For(&'static str),
    /// The flag clear and no field at all — a writer that omitted it. Legal, and
    /// unprovable: nothing here can distinguish a right password from a wrong
    /// one.
    Absent,
    /// The flag set, twelve bytes present, and a tag that is **not** their
    /// SHA-256.
    ///
    /// The hostile shape, and the one that matters most: such a value refutes
    /// *no* password, so a router that read it as a check would find its very
    /// first candidate "verified" and hand a wrong key to the header parse.
    /// `header::encryption::parse` degrades it to `None` before anyone can, and
    /// `None` reads as `Unverifiable`.
    ForgedTag(&'static str),
}

/// A RAR5 type-4 archive encryption header — plaintext, first thing after the
/// signature, exactly as `-hp` writes it.
///
/// Body: version, flags, the KDF count as a raw byte, the 16-byte salt, and —
/// when the flags claim one — the 8-byte password check followed by the first
/// four bytes of its SHA-256.
fn build_test_rar_crypt_header(kdf_lg2: u8, check: HeaderCheck) -> Vec<u8> {
    let checked = |password: &str| {
        unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, kdf_lg2)
            .expect("the fixture KDF count is derivable")
            .psw_check
    };
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(0)); // AES-256.
    type_body.extend_from_slice(&encode_test_rar_vint(u64::from(!matches!(
        check,
        HeaderCheck::Absent
    ))));
    type_body.push(kdf_lg2);
    type_body.extend_from_slice(&TEST_HP_SALT);
    match check {
        HeaderCheck::Absent => {}
        HeaderCheck::For(password) => {
            let value = checked(password);
            type_body.extend_from_slice(&value);
            type_body.extend_from_slice(&<sha2::Sha256 as sha2::Digest>::digest(value)[..4]);
        }
        HeaderCheck::ForgedTag(password) => {
            type_body.extend_from_slice(&checked(password));
            // Four bytes that are not anyone's SHA-256 prefix.
            type_body.extend_from_slice(&[0u8; 4]);
        }
    }
    build_test_rar_header(4, 0, &type_body, &[])
}

/// Wraps one plaintext header the way `-hp` stores it:
/// `[16-byte IV][AES-256-CBC(header padded to 16)]`.
///
/// The padding's content is irrelevant to the parser — the CRC covers the size
/// vint and the body only, and the body is read by its declared length — which
/// is why zeros are as faithful as random bytes here.
fn seal_test_rar_header(key: &[u8; 32], iv: &[u8; 16], header: &[u8]) -> Vec<u8> {
    let mut block = header.to_vec();
    block.resize(header.len().div_ceil(16) * 16, 0);
    unrar_rs::encrypt_cipher_range(key, iv, &mut block)
        .expect("the padded header is block-aligned");
    let mut out = iv.to_vec();
    out.extend_from_slice(&block);
    out
}

/// One `-hp -m0` stored member split across `volume_count` volumes.
///
/// Mirrors what `rar a -m0 -hp<password> -v<size>` writes: the type-4 record in
/// the clear at the front of every volume, every header after it AES-256-CBC
/// under the archive key with its own inline IV, and the member's data area
/// **not** header-encrypted — it is the ordinary `-p` cipher stream, one
/// unbroken AES-CBC run across the volume boundaries, keyed from the file
/// header's own `FHEXTRA_CRYPT` record. `-hp` is `-p` plus encrypted headers,
/// and one password opens both.
///
/// - `password` keys everything: the headers, the file data, and the checks.
/// - `check` is what the *archive-level* record claims, which is what `-hp`
///   admission is decided on. The member-level check is always this password's,
///   because a member that refuted it would be testing the admission gate
///   rather than this one.
fn header_encrypted_store_set(
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
    password: &'static str,
    check: HeaderCheck,
) -> Vec<(String, Vec<u8>)> {
    let header_key = unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, TEST_HP_KDF_LG2)
        .expect("the fixture KDF count is derivable")
        .key;
    let member = unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
        .expect("the fixture KDF count is derivable");

    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut cipher = payload.to_vec();
    cipher.resize(cipher_len, 0);
    unrar_rs::encrypt_cipher_range(&member.key, &TEST_CRYPT_IV, &mut cipher)
        .expect("the padded payload is block-aligned");
    let member_crc = unrar_rs::convert_crc32_to_mac(checksum::crc32(payload), &member.hash_key);

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
            let data_crc = if is_last {
                member_crc
            } else {
                checksum::crc32(part)
            };
            let extra = build_test_rar_crypt_extra(Some(&member.psw_check), is_last);

            // A distinct IV per header, as a real writer emits.
            let mut iv = [0u8; 16];
            let seal = |index: u8, iv: &mut [u8; 16], header: &[u8]| {
                iv.fill(0x40u8.wrapping_add(index).wrapping_add(volume as u8 * 8));
                seal_test_rar_header(&header_key, iv, header)
            };

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_crypt_header(TEST_HP_KDF_LG2, check));
            bytes.extend_from_slice(&seal(
                0,
                &mut iv,
                &build_test_rar_main_header(
                    if is_first { 0x0001 } else { 0x0001 | 0x0002 },
                    (!is_first).then_some(volume as u64),
                ),
            ));
            bytes.extend_from_slice(&seal(
                1,
                &mut iv,
                &build_test_rar_file_header_with_extra(
                    member_name,
                    split_flags,
                    part.len() as u64,
                    payload.len() as u64,
                    Some(data_crc),
                    &extra,
                ),
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&seal(2, &mut iv, &build_test_rar_end_header(!is_last)));

            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// [`header_encrypted_store_set`] plus an unsplit, encrypted, BLAKE2sp-only
/// member in the closing volume — the shape that puts a **tolerated** member
/// inside an `-hp` set.
///
/// Both members are encrypted, because `-hp` encrypts the data as well as the
/// headers; what makes the extra one ineligible is that its header states a
/// BLAKE2sp digest and no CRC32, which `classify_stored_chain` answers with
/// `Blake2OnlyNoCrc32` on the encrypted path exactly as on the plaintext one.
/// Unsplit for the reason [`ToleranceExtra::Blake2OnlyStore`] gives: the
/// classifier only reaches the hash fields once the chain closes, so a split
/// one would route into a partial before resolving.
fn header_encrypted_store_set_with_extra_member(
    member_name: &str,
    payload: &[u8],
    extra_name: &str,
    extra_payload: &[u8],
    volume_count: usize,
    password: &'static str,
    check: HeaderCheck,
) -> Vec<(String, Vec<u8>)> {
    let header_key = unrar_rs::derive_rar5_material(password, &TEST_HP_SALT, TEST_HP_KDF_LG2)
        .expect("the fixture KDF count is derivable")
        .key;
    let member = unrar_rs::derive_rar5_material(password, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
        .expect("the fixture KDF count is derivable");

    let mut volumes =
        header_encrypted_store_set(member_name, payload, volume_count, password, check);

    // The extra member's own cipher stream. Same key as the split member — one
    // salt for the set, which is legal and is what the rest of this fixture
    // family does — and `align16` padded, because that is the extent
    // `classify_stored_chain` requires an encrypted chain to sum to.
    let extra_cipher_len = extra_payload.len().div_ceil(16) * 16;
    let mut extra_cipher = extra_payload.to_vec();
    extra_cipher.resize(extra_cipher_len, 0);
    unrar_rs::encrypt_cipher_range(&member.key, &TEST_CRYPT_IV, &mut extra_cipher)
        .expect("the padded payload is block-aligned");

    let mut extra = build_test_rar_crypt_extra(Some(&member.psw_check), false);
    extra.extend_from_slice(&build_test_rar_blake2_extra(
        unrar_rs::crypto::blake2sp_hash(extra_payload),
    ));

    // Rebuild the closing volume with the extra member spliced in ahead of its
    // end header. The end header is re-sealed under the next IV index, which is
    // what keeps the sealed chain contiguous.
    // Sealing is deterministic in the header's length, so the size of the end
    // header already on the tail is what has to come off before the extra
    // member goes on and a fresh one is appended under the next IV index.
    let last = volume_count - 1;
    let iv_at = |index: u8| [0x40u8.wrapping_add(index).wrapping_add(last as u8 * 8); 16];
    let (name, bytes) = volumes[last].clone();
    let end_header = build_test_rar_end_header(false);
    let sealed_end_len = seal_test_rar_header(&header_key, &iv_at(2), &end_header).len();

    let mut rebuilt = bytes[..bytes.len() - sealed_end_len].to_vec();
    rebuilt.extend_from_slice(&seal_test_rar_header(
        &header_key,
        &iv_at(2),
        &build_test_rar_file_header_with_extra(
            extra_name,
            0,
            extra_cipher_len as u64,
            extra_payload.len() as u64,
            None,
            &extra,
        ),
    ));
    rebuilt.extend_from_slice(&extra_cipher);
    rebuilt.extend_from_slice(&seal_test_rar_header(&header_key, &iv_at(3), &end_header));
    volumes[last] = (name, rebuilt);
    volumes
}

/// A RAR4 `-hp` volume: the archive header's `ENCRYPTED_HEADERS` flag and then
/// ciphertext.
///
/// Deliberately not decryptable, and that costs nothing, because RAR4 `-hp` is
/// refused **at the flag** — the format carries no password-check value
/// anywhere, so there is nothing an admission gate could stand on and no
/// candidate can be proved before something is decrypted. What the bytes past
/// the flag are is exactly as irrelevant to this router as it is to a real
/// archive it has no key for.
fn header_encrypted_rar4_set(volume_count: usize, body_bytes: usize) -> Vec<(String, Vec<u8>)> {
    (0..volume_count)
        .map(|volume| {
            let is_first = volume == 0;
            // VOLUME | NEW_NUMBERING | ENCRYPTED_HEADERS, plus FIRST_VOLUME.
            let mut flags = 0x0001u16 | 0x0010 | 0x0080;
            if is_first {
                flags |= 0x0100;
            }
            let mut main_body = Vec::new();
            main_body.extend_from_slice(&0u16.to_le_bytes()); // high_pos_av
            main_body.extend_from_slice(&0u32.to_le_bytes()); // pos_av

            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            bytes.extend_from_slice(&build_test_rar4_block(0x73, flags, &main_body));
            bytes.extend((0..body_bytes).map(|index| ((index * 31 + volume * 17) % 256) as u8));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

/// The `-hp` gate runner: [`run_gate_with_password`] plus the two candidate
/// sources that do not live on the job spec.
///
/// `nzb_zstd` is the job's persisted NZB, which is where `nzb.meta.password`
/// comes from, and `nzb_file_name` is the path whose stem carries a
/// `{{password}}` convention. Both are read by
/// `archive_password_candidates_for_job`, which is the harvest the header key
/// is derived from — so a fixture that sets neither is a job with only whatever
/// `spec.password` holds.
#[allow(clippy::too_many_arguments)]
async fn run_hp_gate(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    nzb_file_name: Option<&str>,
) -> GateOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);

    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir = insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        nzb_zstd,
        nzb_file_name,
    )
    .await;

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
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    GateOutcome {
        member,
        member_location,
        status,
        volume_file_seen,
    }
}

/// What a `-hp` job's routing decided, without driving extraction to terminal,
/// and with the persisted NZB's file name chosen so the `{{password}}`
/// convention can be exercised.
///
/// The refusal twin of [`run_hp_gate`], for the one case where the point is the
/// named demotion and nothing downstream of it.
///
/// It stops at the demotion because that is where its caller's claim ends, and
/// **not** because a refused set has nowhere to go: it very much does, and
/// [`hp_fallback_outcome`] is where that is proved. Every header-encryption
/// refusal reason the set can reach hands its current article back to the
/// conventional path, and for a job that holds the password that path then
/// opens the archive and produces the member. Anything
/// asserting *that* has to use the other helper; this one would report a job
/// still `Downloading`, because nothing here ever re-feeds the refetch the
/// demotion asked for.
async fn hp_routing_outcome_named(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    nzb_file_name: Option<&str>,
) -> EncryptedRoutingOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir = insert_active_job_with_persisted_nzb_named(
        &mut pipeline,
        job_id,
        spec,
        nzb_zstd,
        nzb_file_name,
    )
    .await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let volume_file_seen = volumes
        .iter()
        .any(|(filename, _)| working_dir.join(filename).exists());
    let partial_seen = any_direct_partial(&payload_root(&temp_dir, job_id));
    EncryptedRoutingOutcome {
        shape,
        volume_file_seen,
        partial_seen,
    }
}

/// A refused `-hp` job, followed all the way through the fallback it demoted
/// **into**.
///
/// # Why this exists
///
/// "Refuse and fall back to conventional extraction" is the guarantee the whole
/// `-hp` design leans on — it is the stated reason a check-less archive may
/// refuse rather than guess a key. [`hp_routing_outcome`] only ever observed the
/// first half of it: a demotion by name, and a volume file existing on disk. A
/// volume file existing is not the floor; the *member* coming out of it is.
///
/// So this keeps going. A demotion gives the article still held by the decoder
/// to conventional assembly and re-queues only previously committed coverage
/// that cannot be reconstructed; here [`dispatch_and_submit`] stands in for any
/// such refetch, exactly as the restart tests do.
/// The volumes then materialize, extraction runs, and the job reaches a terminal
/// state — and the volumes are byte-compared against the fixtures, because a
/// handoff that materialized *something* at every path is not the same as one
/// that handed over the archive that was posted.
///
/// `corrected_password` is written into the live [`JobSpec`] after the refusal
/// and before the refetch, for the case where the job genuinely had no usable
/// password at routing time. That is not a contrivance: `setJobPassword` and the
/// NZBGet facade's `*Unpack:Password` both mutate the spec in place, the direct
/// set is documented not to come back from a demotion, and the conventional path
/// re-harvests per volume parse — so this is the one seam through which a
/// late password still produces a member. `None` leaves the job exactly as the
/// router refused it.
struct HpFallbackOutcome {
    /// What the routing decided, read at the demotion and before the refetch.
    routing: EncryptedRoutingOutcome,
    /// Previously direct-owned articles the demotion put back on the queue.
    refetched: Vec<(u32, u32)>,
    /// Whether every source volume the fallback materialized is byte-identical
    /// to the fixture that was posted.
    volumes_byte_exact: bool,
    /// Which of the set's volumes reached disk at all.
    volumes_materialized: usize,
    member: Option<Vec<u8>>,
    member_location: Option<&'static str>,
    status: Option<JobStatus>,
}

async fn hp_fallback_outcome(
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
    corrected_password: Option<&str>,
) -> HpFallbackOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Silver Horizon", volumes);
    spec.password = spec_password.map(str::to_owned);
    let working_dir =
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, nzb_zstd).await;
    // Dispatched rather than merely submitted, unlike every other `-hp` helper
    // here: the article has to leave the download queue on its way in, or the
    // job's *original* queue is still sitting there at demotion time and
    // "everything the demotion re-queued" would read every article back whether
    // the refetch ran or not. This is the difference between observing the
    // handoff and observing the harness.
    let mut corrected = false;
    for (file_index, segment_number) in arrivals {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            volumes,
            *file_index,
            *segment_number,
            2,
        )
        .await;
        // The operator's correction, applied the instant the refusal becomes
        // visible — which is where it happens in life, because that refusal is
        // what tells them a password is needed. It has to be in the spec before
        // the *later* volumes finish downloading: the conventional path harvests
        // per volume parse, and a `-hp` volume parsed without a password yields
        // no topology at all, so a correction applied after the last article
        // would leave the archive undetected rather than unextracted.
        if !corrected
            && let Some(password) = corrected_password
            && pipeline
                .direct_store
                .sets_for(job_id)
                .iter()
                .any(|set| set.is_demoted())
        {
            pipeline
                .jobs
                .get_mut(&job_id)
                .expect("the job is still active")
                .spec
                .password = Some(password.to_string());
            corrected = true;
        }
    }
    drain_rar_refreshes(&mut pipeline).await;
    assert!(
        corrected == corrected_password.is_some(),
        "a caller that supplied a corrected password expects a refusal to apply it to"
    );

    let routing = EncryptedRoutingOutcome {
        shape: format!("{:?}", pipeline.direct_store.sets_for(job_id)),
        volume_file_seen: volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        partial_seen: any_direct_partial(&payload_root(&temp_dir, job_id)),
    };

    // Sampled as the refetch runs, not read at the end: a *successful* fallback
    // extraction deletes the source volumes it consumed, so by the time the job
    // is terminal there is nothing left on disk to compare. The largest image
    // ever observed for each volume is the one the extractor was handed.
    let mut materialized: Vec<Option<Vec<u8>>> = vec![None; volumes.len()];
    let sample = |materialized: &mut Vec<Option<Vec<u8>>>| {
        for (index, (filename, _)) in volumes.iter().enumerate() {
            let Ok(bytes) = std::fs::read(working_dir.join(filename)) else {
                continue;
            };
            if materialized[index]
                .as_ref()
                .is_none_or(|seen| seen.len() < bytes.len())
            {
                materialized[index] = Some(bytes);
            }
        }
    };
    sample(&mut materialized);

    // Any refetch the demotion still needs. Looped because materializing one
    // volume can put the next one's articles back on the queue, and bounded so a
    // pipeline that re-queued forever fails here rather than spinning.
    let refetched = peek_queued_segments(&mut pipeline, job_id);
    for _ in 0..8 {
        let queued = peek_queued_segments(&mut pipeline, job_id);
        if queued.is_empty() {
            break;
        }
        for (file_index, segment_number) in queued {
            dispatch_and_submit(
                &mut pipeline,
                job_id,
                volumes,
                file_index,
                segment_number,
                2,
            )
            .await;
            sample(&mut materialized);
        }
    }
    // Extraction is only driven once the fallback has something to extract. A
    // handoff that loses articles leaves the job in `Downloading` forever, and
    // driving it there would spend the harness's three-minute extraction timeout
    // to report a fact the caller's `refetched` and `volumes_byte_exact`
    // assertions state precisely and immediately.
    if materialized.iter().all(Option::is_some) {
        // Stands in for the download worker's own call, which this harness never
        // reaches: `submit_decoded_segment` enters the pipeline at
        // `handle_decode_success`, and it is the *download* side that schedules
        // a completion check once a job's download pipeline drains. The check
        // itself still decides; this only asks the question. (Several other test
        // modules here do exactly the same for the same reason.)
        pipeline.schedule_job_completion_check(job_id);
        drain_rar_refreshes(&mut pipeline).await;
        sample(&mut materialized);
        drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
        sample(&mut materialized);
    }

    let volumes_materialized = materialized.iter().filter(|bytes| bytes.is_some()).count();
    let volumes_byte_exact = volumes_materialized == volumes.len()
        && materialized
            .iter()
            .zip(volumes)
            .all(|(written, (_, posted))| written.as_deref() == Some(posted.as_slice()));

    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    HpFallbackOutcome {
        routing,
        refetched,
        volumes_byte_exact,
        volumes_materialized,
        member,
        member_location,
        status: job_status_for_assert(&pipeline, job_id),
    }
}

#[tokio::test]
async fn a_header_encrypted_set_keyed_from_nzb_meta_matches_the_conventional_extractor() {
    // The header-encryption spine. The password is in the NZB's `<meta
    // type="password">` and nowhere else — no operator ever typed it — and the
    // set routes from its first article.
    let member_name = "Silver.Horizon.S04E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    // Non-vacuity on the fixture itself: without a password these volumes state
    // nothing at all, which is what makes this a `-hp` test rather than a `-p`
    // one with extra steps.
    assert!(
        unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(volumes[0].1.clone()), None)
            .is_err(),
        "a `-hp` fixture must yield no facts without a password"
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_hp_gate(
        DirectStoreGate::Disabled,
        JobId(45001),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;
    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45002),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert!(
        !direct.volume_file_seen,
        "a routed `-hp` set must never create a source volume file"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a `-hp` set routed direct must be byte-identical to the conventional extractor, in \
         the same directory, with the same job status"
    );
}

#[tokio::test]
async fn a_tolerated_member_of_a_header_encrypted_set_extracts_with_the_proved_password() {
    // The small-member tolerance extracts a small ineligible member through the
    // hybrid provider — and an `-hp` set's *virtual* volumes are as
    // header-encrypted as the posted ones, so that extraction cannot so much as
    // open the archive without the key the router proved. It used to open with
    // no password at all, which failed at the first header and demoted the set
    // under `ToleratedExtractionFailed`: correct output by way of a full
    // materialize and a conventional re-extract, for a set that had already
    // routed every stored byte.
    //
    // The extra member is BLAKE2sp-only, which `classify_stored_chain` answers
    // with `Blake2OnlyNoCrc32` on the encrypted path exactly as on the plaintext
    // one, and `tolerated_member_names` takes any `Ineligible(_)`.
    let member_name = "Silver.Horizon.S04E09.mkv";
    let extra_name = "Silver.Horizon.S04E09.nfo";
    // The store member has to be at least 100x the extra for the extra to fit
    // under `min(64 MiB, 1% of packed archive bytes)`.
    let payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = header_encrypted_store_set_with_extra_member(
        member_name,
        &payload,
        extra_name,
        &extra_payload,
        4,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    // Non-vacuity on the fixture: these volumes state nothing without a
    // password, so the tolerated extraction genuinely needs one.
    assert!(
        unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(volumes[0].1.clone()), None)
            .is_err(),
        "a `-hp` fixture must yield no facts without a password"
    );
    let arrivals = in_order_arrivals(volumes.len());

    // Both gates report the *tolerated* member, which is the one under test.
    let conventional = run_hp_gate(
        DirectStoreGate::Disabled,
        JobId(45021),
        extra_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;
    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45022),
        extra_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the tolerated member"
    );
    // The load-bearing pair. Without the password the tolerated extraction fails
    // and the set demotes, and a demotion materializes every source volume — so
    // this assertion is what tells a tolerated extraction that *worked* from one
    // that was rescued by the fallback.
    assert!(
        !direct.volume_file_seen,
        "the tolerated extraction must succeed in place, not by demoting the set"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a tolerated member of a routed `-hp` set must match the conventional extractor"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_keys_from_the_filename_password_convention() {
    // The third harvest source, and the one that needs no NZB body at all: the
    // password is in the NZB *file name*'s `{{…}}` stem convention.
    let member_name = "Silver.Horizon.S04E02.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 241) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "harbour-lights",
        HeaderCheck::For("harbour-lights"),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45011),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd(),
        Some("Silver Horizon {{harbour-lights}}.nzb"),
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the filename convention is a candidate source like any other"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));

    // Non-vacuity: **the same job with the `{{…}}` removed from the NZB's file
    // name** — same archive, same payload, same empty spec password, same
    // password-free NZB body — has no candidate at all and refuses by name.
    // Without this, a run that ignored the filename and keyed from somewhere
    // else entirely would pass above.
    let refused = hp_routing_outcome_named(
        JobId(45012),
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd(),
        Some("Silver Horizon.nzb"),
    )
    .await;
    assert!(
        refused
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "with the convention gone there is no candidate at all, got {}",
        refused.shape
    );
    assert!(
        refused.volume_file_seen,
        "and the set must have fallen back to materializing its volumes"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_with_the_wrong_password_demotes_by_name() {
    // And then the operator corrects it, which is the half that matters. A
    // refusal here is only survivable because the set leaves direct mode
    // *intact*: the in-hand article continues conventionally, the volumes
    // materialize byte for byte, and that path — which re-harvests the job's
    // passwords per volume parse rather than memoizing them — opens the archive
    // with the corrected one and produces the member. `setJobPassword` and the
    // NZBGet facade's `*Unpack:Password` are the real mutations this stands in
    // for, and the direct set deliberately does not come back from either.
    let member_name = "Silver.Horizon.S04E03.mkv";
    let payload: Vec<u8> = (0..2000u32).map(|index| (index % 233) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_fallback_outcome(
        JobId(45021),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("not-the-password"),
        sample_nzb_zstd(),
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoVerifiedCandidate))"),
        "the archive's own check refutes every candidate, and the refusal must say so, got {}",
        outcome.routing.shape
    );
    assert!(
        !outcome.routing.partial_seen,
        "nothing may have been written on the strength of a refuted password"
    );

    assert!(
        outcome.refetched.is_empty(),
        "the refusal lands on the first parse, so its live article must continue \
         conventionally without a refetch; got {:?}",
        outcome.refetched
    );
    assert!(
        outcome.volumes_byte_exact,
        "and the volumes it materializes are the pre-E4 floor: the archive exactly as \
         posted, not an approximation of it — {} of {} reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "a password that arrives after the refusal still has to produce the member, through \
         the conventional path the refusal handed the set to"
    );
    assert_eq!(outcome.member_location, Some("complete"));
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_with_no_password_demotes_by_name() {
    let member_name = "Silver.Horizon.S04E04.mkv";
    let payload: Vec<u8> = (0..2000u32).map(|index| (index % 199) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_fallback_outcome(
        JobId(45031),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        None,
        sample_nzb_zstd(),
        // Supplied after the refusal, which is the ordinary shape of this case:
        // a passworded post nobody labelled, refused in seconds rather than
        // after `MAX_HEADER_PREFIX_BYTES` of staging, and typed in later.
        Some("moonlit-harbour"),
    )
    .await;

    // Named, and named *at the first parse*. Before the named refusal this
    // volume's articles simply staged: `parse_volume_facts` failed, the router
    // read that as "the prefix is too short", and the set sat un-demoted until
    // it had burned `MAX_HEADER_PREFIX_BYTES`. So an assertion on the name is
    // also an assertion that the parse failure was recognised rather than
    // swallowed — the old code reaches no `Demoted(...)` here at all.
    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "a `-hp` set with no candidate must demote by name, got {}",
        outcome.routing.shape
    );
    assert!(
        !outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoVerifiedCandidate))"),
        "\"nothing to try\" and \"everything tried was wrong\" are different operational \
         stories, got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // Refusing early is only better than waiting because the wait was never
    // buying anything: the set still reaches the conventional path with every
    // byte intact, and a password supplied afterwards still produces the member.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert_eq!(outcome.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(outcome.member_location, Some("complete"));
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_does_not_try_a_placeholder_spec_password() {
    // `spec.password` reaches the `-hp` ring by a different route from the rest
    // of the harvest — the per-article re-offer in `refresh_direct_passwords` —
    // and it has to be normalized the way
    // `archive_password_candidates_for_job` normalizes, or the two routes
    // disagree about what a password even *is*.
    //
    // The case is not hypothetical: indexers have written `password=yes` since
    // the NZBGet era to mean "this post is passworded", and
    // `normalize_archive_password_candidate` drops that whole family. Offering
    // it here anyway would spend a PBKDF2 derivation proving nothing — and
    // would report the refusal as `NoVerifiedCandidate`, "everything we tried
    // was wrong", for a job that had nothing to try.
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 163) as u8).collect();
    let volumes = header_encrypted_store_set(
        "Silver.Horizon.S04E14.mkv",
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_routing_outcome_named(
        JobId(45131),
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("yes"),
        sample_nzb_zstd(),
        None,
    )
    .await;

    assert!(
        outcome
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "a placeholder is not a candidate, so this job had nothing to try, got {}",
        outcome.shape
    );
    assert!(
        !outcome.shape.contains("NoVerifiedCandidate"),
        "and it must not be reported as a password that was tried and refuted, got {}",
        outcome.shape
    );
}

/// **The `Verified`-only decision, as a test.**
///
/// Both fixtures below carry an archive whose password the job *has* — the right
/// one, in the spec, ready to use — and both must still refuse, because neither
/// states a check that could prove it. That is the whole difference from `-p`,
/// where the same `Unverifiable` verdict admits: a wrong key there corrupts data
/// the whole-member CRC32 then catches, and a wrong key here corrupts the header
/// parse that decides where bytes go.
///
/// If admission were relaxed to "admit on `Unverifiable`", both halves would
/// pass their parse and complete — so this test fails loudly rather than
/// quietly, and it fails for the exact change it is guarding.
///
/// # And the refusal is only defensible because the floor holds
///
/// Refusing an archive the job *can* open is a real cost, and the argument that
/// it is the right trade is entirely about what happens next: the conventional
/// path takes the set and opens it with the very same password. So each half
/// runs that through — the refetch the demotion asked for, the volumes it
/// materializes, the extraction, the member. Asserting only that a volume file
/// appeared would leave the trade unproven in the one test whose whole subject
/// is the trade.
#[tokio::test]
async fn a_header_encrypted_set_with_no_usable_check_refuses_rather_than_guessing() {
    let member_name = "Silver.Horizon.S04E05.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 211) as u8).collect();
    for (label, check) in [
        ("an omitted check", HeaderCheck::Absent),
        (
            "a check whose own tag is wrong",
            HeaderCheck::ForgedTag("moonlit-harbour"),
        ),
    ] {
        let volumes =
            header_encrypted_store_set(member_name, &payload, 2, "moonlit-harbour", check);
        let outcome = hp_fallback_outcome(
            JobId(45041),
            member_name,
            &volumes,
            &in_order_arrivals(volumes.len()),
            // The **right** password, in hand, before the first article.
            Some("moonlit-harbour"),
            sample_nzb_zstd(),
            None,
        )
        .await;

        assert!(
            outcome
                .routing
                .shape
                .contains("Demoted(HeaderEncryptedRefused(Unverifiable))"),
            "{label}: an unprovable `-hp` archive must refuse even when the password is \
             right, got {}",
            outcome.routing.shape
        );
        assert!(
            !outcome.routing.partial_seen,
            "{label}: nothing may have been written on the strength of an unproved password"
        );

        // The floor, run rather than assumed.
        assert!(
            outcome.refetched.is_empty(),
            "{label}: the decoder must hand its live article directly to conventional \
             assembly rather than refetching it; got {:?}",
            outcome.refetched
        );
        assert!(
            outcome.volumes_byte_exact,
            "{label}: and the volumes it then materializes must be the archive that was \
             posted, byte for byte — {} of {} reached disk",
            outcome.volumes_materialized,
            volumes.len()
        );
        assert_eq!(
            outcome.member.as_deref(),
            Some(payload.as_slice()),
            "{label}: and the conventional extractor must open it with the same password the \
             `-hp` gate refused to guess with, and produce the member"
        );
        assert_eq!(outcome.member_location, Some("complete"), "{label}");
        assert!(
            matches!(outcome.status, Some(JobStatus::Complete)),
            "{label}: a refused `-hp` job must reach a terminal state, got {:?}",
            outcome.status
        );
    }

    // Non-vacuity, against the same payload and the same password: the *only*
    // thing that changed is the archive stating a usable check, and with it the
    // set routes to completion. Without this the two halves above would pass
    // against a build that refused every `-hp` set for any reason at all.
    let member_name = "Silver.Horizon.S04E05.mkv";
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    let routed = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45042),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("moonlit-harbour"),
        sample_nzb_zstd(),
        None,
    )
    .await;
    assert!(!routed.volume_file_seen);
    assert_eq!(routed.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(routed.member_location, Some("complete"));
}

#[tokio::test]
async fn a_rar4_header_encrypted_set_refuses_by_name() {
    // Permanent, and not a gap to be filled later: RAR4 derives a fresh key per
    // header from that header's own 8-byte salt and carries no password-check
    // value anywhere, so a wrong password is detected only by walking off the
    // end of the archive.
    let volumes = header_encrypted_rar4_set(2, 4096);
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );

    let outcome = hp_fallback_outcome(
        JobId(45051),
        "Silver.Horizon.S04E09.mkv",
        &volumes,
        &in_order_arrivals(volumes.len()),
        // The password is present and correct-looking. It changes nothing,
        // which is the point: there is nothing to prove it against.
        Some("moonlit-harbour"),
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(Rar4Headers))"),
        "RAR4 `-hp` must refuse under its own name rather than under a wrong-password one, \
         got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // The floor, for the one refusal that is permanent. No member is asserted
    // and none could be: this fixture is a RAR4 `-hp` header over bytes that are
    // deliberately not decryptable, because RAR4 offers nothing to decrypt them
    // *against* — which is the whole reason the refusal exists. What is asserted
    // is the handoff, and it is the entire remedy available: the conventional
    // path is given the archive exactly as posted, and reaches its own terminal
    // verdict on it rather than leaving the job wedged.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "an archive nothing can open must *fail*, not hang: got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_whose_kdf_count_is_over_the_ceiling_refuses_by_name() {
    // `lg2_count` is the *archive's* claim, so an unbounded one would let a
    // hostile post choose how much PBKDF2 an admission decision costs.
    // `unrar-rs` bounds it at `CRYPT5_KDF_LG2_COUNT_MAX` — RAR's own limit —
    // before it even reads the salt, and naming the refusal here is what stops
    // such a volume from also burning `MAX_HEADER_PREFIX_BYTES` of staging.
    let over = unrar_rs::CRYPT5_KDF_LG2_COUNT_MAX + 1;
    let volumes: Vec<(String, Vec<u8>)> = (0..2usize)
        .map(|volume| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_crypt_header(over, HeaderCheck::Absent));
            bytes.extend((0..4096usize).map(|index| ((index * 29 + volume * 7) % 256) as u8));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect();

    let outcome = hp_fallback_outcome(
        JobId(45061),
        "Silver.Horizon.S04E10.mkv",
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("moonlit-harbour"),
        sample_nzb_zstd(),
        None,
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(Unkeyable))"),
        "an archive demanding a derivation this build refuses must be named as such, got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // As with the RAR4 refusal: no member is asserted because the fixture is a
    // hostile type-4 record over filler and there is none, but the handoff is —
    // refusing the *derivation* must still preserve the posted bytes.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_routes_payload_that_lands_before_its_volume_header() {
    // Out-of-order arrival, with the extra `-hp` twist: the article carrying
    // the type-4 record — without which nothing can be keyed at all — is the
    // *last* thing to arrive for every volume. Every byte before it has to be
    // retained, unclassifiable, and drain once the key exists.
    let member_name = "Silver.Horizon.S04E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));

    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45071),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "bytes retained while the set was unkeyed must not have been materialized"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "held bytes must drain to their destination once the header key resolves"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn a_password_harvest_that_failed_does_not_arm_the_once_per_job_memo() {
    // The `-hp` harvest runs **once** per job, and what arms that memo decides
    // how a transient error is paid for. Its NZB half is a database read
    // followed by a parse, and both warn-and-continue with an empty list, so an
    // empty result is two different facts: "this job names no password", which
    // is permanent, and "the read failed this once", which is not.
    //
    // Both halves below are load-bearing and they pull in opposite directions:
    //
    // - Arming on a **failed** harvest costs the job its `NzbMeta` and
    //   `FilenameConvention` candidates for the rest of its life, with no second
    //   chance — `wants_header_password()` is the only other gate and it is still
    //   true. The conventional path has no such cliff: it re-harvests per volume
    //   parse.
    // - Not arming on an **empty but successful** one costs a persisted-NZB read,
    //   a zstd decompress and an XML parse on *every article of every
    //   password-free job*, which is very nearly every job there is.
    //
    // So the rule is "remember what was learned, and only that", and only the
    // memo itself can say which happened: the two runs are otherwise identical
    // and both end with zero candidates offered.
    let member_name = "Silver.Horizon.S04E13.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 173) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    for (label, nzb_zstd, expect_armed) in [
        (
            "a persisted NZB that cannot be read",
            vec![0xFFu8; 64],
            false,
        ),
        (
            "a persisted NZB that reads and names no password",
            sample_nzb_zstd(),
            true,
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let job_id = JobId(45121);
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let spec = direct_store_job_spec("Silver Horizon", &volumes);
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, nzb_zstd).await;

        // The *payload* article only. It is enough to run the harvest — the
        // routing seam calls it before it does anything with the bytes — and not
        // enough for any volume to have reached its type-4 record, so the set is
        // still asking for candidates and the memo is the only difference
        // between the two runs.
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
        assert!(
            pipeline
                .direct_store
                .set(job_id, 0)
                .is_some_and(|set| set.router.wants_header_password()),
            "{label}: the set must still want candidates, or something other than the memo \
             is what stops the harvest re-running"
        );
        assert_eq!(
            pipeline.direct_store.header_candidates_offered(job_id),
            expect_armed,
            "{label}: a harvest is remembered when it ran and forgotten when it failed"
        );
    }
}

#[tokio::test]
async fn a_par2_bearing_header_encrypted_job_verifies_and_completes_byte_identically() {
    // The commonest real `-hp` shape, and the one every other test here builds
    // without: nearly every encrypted release carries PAR2, and both `-hp`
    // helpers switch live verification off.
    //
    // Nothing about `-hp` should reach the verifier — the archive key is used
    // to read *headers*, and PAR2 describes the posted volume image, which is
    // ciphertext either way. That is precisely the claim worth a test, because
    // it is the sort of claim that stays true only until something reads a
    // plaintext byte where a posted one belongs. The overlay answers the
    // verifier out of the routed member partials re-encrypted, and this asserts
    // that a `-hp` set stays in that arrangement all the way through
    // verification: it admits, it never writes a source volume, it reaches a
    // PAR2 verdict, it finalizes, and its member is byte-identical to the one
    // the conventional extractor produces from real volume files.
    let member_name = "Silver.Horizon.S04E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(45111),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(45112),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );

    assert!(
        direct.admitted,
        "a par2-bearing `-hp` job must still admit its set, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "a routed `-hp` set must never create a source volume file, even to verify against, \
         got {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set must have stayed direct through verification and finalized, got {}",
        direct.demotions
    );
    assert!(
        direct.verdict_reached,
        "the job must have reached a PAR2 verdict rather than skipping the question; \
         authoritative={}",
        direct.authoritative_verify_calls
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
        "a par2-bearing `-hp` job must produce the conventional gate's output, in the same \
         place, with the same status; sets = {}",
        direct.demotions
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "got {:?} with sets {}",
        direct.status,
        direct.demotions
    );
}

#[tokio::test]
async fn a_header_encrypted_set_keys_from_a_password_supplied_after_its_harvest_ran() {
    // **The re-offer in `refresh_direct_passwords`, as a test.**
    //
    // `offer_direct_header_passwords` runs once per job and every non-demoted
    // set wants a header password from creation, so the harvest is memoized on
    // the job's *first article* — whatever the set later turns out to be — and
    // can never run again. That is fine for the two candidate sources it reads,
    // `NzbMeta` and `FilenameConvention`, because both are immutable per job.
    // It is not fine for the third: `spec.password` is mutable, through
    // `setJobPassword` and through the NZBGet facade's `*Unpack:Password`, and
    // the per-article re-offer is its *only* route into the `-hp` ring after
    // that first article.
    //
    // Reaching that window needs two things at once, and this is the arrival
    // plan that produces them:
    //
    // 1. **The job holds no password anywhere** when its first article lands —
    //    empty spec, password-free NZB body, no `{{…}}` in the NZB's file name —
    //    so the harvest runs, finds nothing, and arms its memo on an empty list.
    // 2. **No volume has yielded its type-4 record yet**, so the `-hp` gate has
    //    not been asked to decide and has therefore not refused. Every volume's
    //    *payload* article arrives first; the record lives at the front of the
    //    volume, in the article that comes last.
    //
    // Only then does the operator supply the password. With the re-offer, the
    // header articles key the set and it completes; without it, the ring is
    // still holding zero candidates when the first record parses, and the set
    // refuses under `NoPassword` for a password the job was holding.
    let member_name = "Silver.Horizon.S04E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 227) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45101);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // No password in the spec, none in the NZB body, and the NZB's name is the
    // job id — so `archive_password_candidates_for_job` has nothing to return.
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    assert!(spec.password.is_none());
    let working_dir =
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, sample_nzb_zstd()).await;

    for file_index in 0..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 1).await;
    }

    // The window, stated: the harvest has run and can never run again, and the
    // ring is still open because nothing has parsed a record to decide against.
    assert!(
        pipeline.direct_store.header_candidates_offered(job_id),
        "the once-per-job harvest must already have run — that is what makes the re-offer \
         the only route left"
    );
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "no volume has reached its type-4 record yet, so nothing can have been refused"
    );
    assert!(
        set.router.wants_header_password(),
        "and the ring must still be collecting candidates"
    );

    // The operator supplies it, mid-download.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is still active")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for file_index in 0..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 0).await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a password the job was holding when the record parsed must reach the `-hp` ring, \
         got {shape}"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a set keyed from a late password still routes, so no source volume may appear"
    );
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "and the member it produces is the ordinary one"
    );
    assert_eq!(member_location, Some("complete"));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn a_header_encrypted_set_restarted_mid_download_completes_byte_identically() {
    // The archive key is never persisted, so a restart has to prove one of the
    // job's candidates again — and for the volume that **closes the chain** it
    // has to do it from the *envelope*: a restored volume's staged image has a
    // hole from offset zero, so the live parse path never reaches the type-4
    // record through it, and a closing volume cannot be confirmed structurally
    // the way a split one can (its last member does not continue, so an
    // undiscovered header could sit past its data area).
    //
    // The arrival plan is shaped for exactly that. Volumes 0 and 1 finish
    // before the restart; the **last** volume is the half-done one, so the
    // resumed run reaches `reconfirm_restored_volume` — and with it the
    // envelope-side keying — rather than the structural short-circuit a middle
    // volume takes.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S04E07.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45081);
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
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the pre-restart run must already have routed rather than materialized"
    );

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
        .expect("the restored `-hp` job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that can still prove a candidate must re-admit the set"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued
            .iter()
            .any(|(file_index, _)| *file_index == 0 || *file_index == 1),
        "volumes 0 and 1 were complete at the barrier; none of their articles may be \
         refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(2, 0)),
        "the closing volume's *first* article must stay below its floor — that is what \
         leaves the resumed run's staged image holed at offset zero, and with it the \
         type-4 record reachable only through the envelope. Got {queued:?}"
    );
    assert!(
        queued.iter().any(|(file_index, _)| *file_index == 2),
        "non-vacuity: the closing volume really was still owed articles, got {queued:?}"
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
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted `-hp` set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted `-hp` set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_prefers_the_proved_candidate_over_the_spec_password() {
    // RAR uses **one** password for headers and file data alike, so the
    // candidate the archive's own check proved is the set's password — even
    // when `spec.password` holds a different one. An operator's wrong guess
    // takes priority in the spec; letting it reach the member key would open the
    // headers and then refuse the members under `EncryptedMemberRefused`.
    //
    // # The window this has to land in, and why an in-order run misses it
    //
    // [`DirectSetRouter::set_password`]'s guard — "a proved archive key is not
    // overwritten" — can only fire between two events: the plaintext type-4
    // record being staged, which is what *proves* the key, and the first
    // **file** header being staged, which admits the member and closes
    // `KeyRing`'s own door. Two articles per volume put both inside article 0:
    // `KeyRing::set_password`'s `admitted` check then blocks every later
    // overwrite by itself, the guard is never reached, and deleting it changes
    // nothing at all.
    //
    // So the arrival plan is sized to hold that window open. `ARTICLES` puts
    // article 0's boundary past the type-4 record and *inside* the encrypted
    // file header: the set keys itself on article 0 and admits nothing, and
    // `refresh_direct_passwords` on article 1 then offers `spec.password` to a
    // router that has already proved a different one. The three assertions
    // between the two articles are what keep it there — without them a fixture
    // that drifted back to "all in article 0" would pass while testing nothing.
    const ARTICLES: usize = 14;
    let member_name = "Silver.Horizon.S04E08.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 181) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    let crypt_record_end = TEST_RAR5_SIG.len()
        + build_test_rar_crypt_header(TEST_HP_KDF_LG2, HeaderCheck::For("moonlit-harbour")).len();
    let first_article_end = article_extent(volumes[0].1.len(), 0, ARTICLES).1;
    assert!(
        first_article_end > crypt_record_end,
        "article 0 has to reach the plaintext type-4 record — it ends at {first_article_end} \
         and the record ends at {crypt_record_end} — or nothing is keyed and the guard is not \
         what is under test"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45091);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    // The operator's guess, which is wrong and which the spec prefers.
    spec.password = Some("not-the-password".to_string());
    let working_dir = insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        // The NZB's, which is right.
        sample_nzb_zstd_with_password("moonlit-harbour"),
    )
    .await;

    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the `-hp` job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "article 0 must have keyed the set from the NZB's password, not refused it"
    );
    assert!(
        !set.router.wants_header_password(),
        "the archive key must already be **proved** after article 0 — a ring still asking for \
         candidates has proved nothing, and there is no guard to reach"
    );
    assert!(
        !set.router.routes_encrypted(),
        "and no member may be admitted yet: the moment one is, `KeyRing`'s own `admitted` \
         check refuses every later password and the guard becomes unreachable. This is the \
         window the guard exists for."
    );

    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..ARTICLES as u32 {
            if (file_index, segment_number) == (0, 0) {
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
    drain_rar_refreshes(&mut pipeline).await;

    // Read before extraction is driven, both because a completed job has its
    // direct-store runtime pruned and because this is the assertion that names
    // the guard: a spec password that displaced the proved one opens the headers
    // and then refuses the *members*, which is a demotion under a wholly
    // different reason.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the spec's password must never displace one the archive's own check proved, got {shape}"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "the set routed throughout and must never have materialized a source volume"
    );
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the proved candidate must key the member data too, and must survive every later \
         offer of the spec's"
    );
    assert_eq!(member_location, Some("complete"));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

/// A volume's articles arrive in whatever order twelve connections finish
/// them, so several mid-file articles routinely land before the one carrying
/// offset zero. The unparsable ceiling must judge the prefix the header walk
/// can actually consume — not the sum of tail chunks the walk cannot reach.
/// Before the fix this exact sequence demoted a store-method set
/// `UnparsableVolume` with its headers never read: six tail articles staged
/// ~4.2 MiB, segment zero arrived seventh, and the ceiling fired before the
/// first parse ever ran. (Witnessed live as 23 of 44 demotions in one
/// functional-direct run, mislabeling compressed sets and falsely demoting
/// the store sets direct routing exists to carry.)
#[tokio::test]
async fn out_of_order_arrival_does_not_trip_the_header_prefix_ceiling() {
    // Payload comfortably past MAX_HEADER_PREFIX_BYTES so the tail articles
    // alone exceed the ceiling the old measure judged.
    let payload: Vec<u8> = (0..(6 * 1024 * 1024) as u32)
        .map(|index| (index % 251) as u8)
        .collect();
    let volumes = single_member_store_set("Silver.Horizon.S01E07.mkv", &payload, 1);
    let articles = 9usize;
    let job_id = JobId(41077);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = direct_store_job_spec_with_articles("Out-of-order store set", &volumes, articles);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let (_, volume_bytes) = &volumes[0];
    // Segments 1.. land first; segment zero arrives last.
    let order: Vec<u32> = (1..articles as u32).chain(std::iter::once(0)).collect();
    for segment in order {
        let (start, end) = article_extent(volume_bytes.len(), segment, articles);
        submit_decoded_segment(
            &mut pipeline,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            segment,
            start as u64,
            &volume_bytes[start..end],
            &volumes[0].0,
            None,
        )
        .await;
        let state = format!("{:?}", pipeline.direct_store.sets_for(job_id));
        assert!(
            !state.contains("Demoted"),
            "the set must never demote on arrival order alone; after segment \
             {segment}: {state}"
        );
    }
}

// ---------------------------------------------------------------------------
// The dual-CRC grid, fed from the direct seam
//
// A direct set's source volumes leave the conventional decode path before its
// commit seam, so until the routing seam fed the grid itself a direct volume
// carried no block verdict at all — and every PAR2 pass over a direct set read
// every byte back through the virtual-volume adapter to learn what the decode
// pass already knew.
//
// Two preconditions run through all of these, and both are production shapes:
// the recovery set has to be **parsed before the volumes decode**, because that
// is where the block size a decoder cuts on comes from; and the articles have to
// carry block-grid CRC segments, because a whole-article record tiles no block
// it does not exactly coincide with.
// ---------------------------------------------------------------------------

/// Articles per volume in the grid fixtures.
const GRID_ARTICLES: usize = 2;

/// The CRC segments a decoder emits once the recovery set's block size is
/// known: one per block boundary the article crosses, based at the article's
/// placement in the file.
fn block_cut_segments(file_offset: u64, data: &[u8], block_size: u64) -> Vec<weaver_yenc::Segment> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        let absolute = file_offset + cursor as u64;
        let to_boundary = (block_size - (absolute % block_size)) as usize;
        let end = (cursor + to_boundary).min(data.len());
        segments.push(weaver_yenc::Segment {
            file_offset: absolute,
            len: (end - cursor) as u64,
            crc32: checksum::crc32(&data[cursor..end]),
        });
        cursor = end;
    }
    segments
}

/// One volume article, carrying the decoder's block-grid segmentation.
///
/// `data` is what the wire delivered, which is the volume's own bytes for an
/// honest arrival and something else for a replay.
#[allow(clippy::too_many_arguments)]
async fn submit_grid_cut_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    file_index: u32,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
) {
    let segments = block_cut_segments(file_offset, data, PAR2_SLICE_BYTES);
    submit_decoded_segment_with_segments(
        pipeline,
        NzbFileId { job_id, file_index },
        segment_number,
        file_offset,
        data,
        filename,
        None,
        true,
        Some(segments),
    )
    .await;
}

/// The article extent one volume ordinal's segment covers, in the fixtures'
/// two-articles-per-volume shape.
fn grid_article_extent(
    volumes: &[(String, Vec<u8>)],
    ordinal: u32,
    segment_number: u32,
) -> (usize, usize) {
    article_extent(
        volumes[ordinal as usize].1.len(),
        segment_number,
        GRID_ARTICLES,
    )
}

/// A par2-bearing direct job whose recovery set parses **before** its volumes
/// arrive, with every volume article carrying block-grid CRC segments.
///
/// The index leads the NZB as well as the wire, so a volume's set-relative
/// index and its NZB file index never coincide — the coordinate confusion that
/// would otherwise pass unnoticed here, where evidence is keyed by one and the
/// grid by the other.
async fn grid_fed_direct_job(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    feed: GridFeed,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    if feed.retained_session {
        // The retained session is off unless the environment turns it on, and a
        // test about that arm must not depend on the ambient default.
        pipeline.stateful_par2_session_forced = Some(true);
    }

    let (spec, index_file_index) = par2_bearing_job_spec_positioned(
        "Silver Horizon",
        volumes,
        par2_bytes,
        IndexPosition::First,
        GRID_ARTICLES,
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, par2_bytes).await;
    assert!(
        pipeline.par2_block_size(job_id).is_some(),
        "non-vacuity: the index has to be parsed before the volumes decode, or the \
         decoder has no grid to cut on and the whole fixture claims nothing"
    );

    let last_volume = volumes.len() as u32 - 1;
    for ordinal in 0..volumes.len() as u32 {
        for segment_number in 0..GRID_ARTICLES as u32 {
            if feed.withhold_last_article
                && ordinal == last_volume
                && segment_number + 1 == GRID_ARTICLES as u32
            {
                continue;
            }
            let (start, end) = grid_article_extent(volumes, ordinal, segment_number);
            let (filename, bytes) = &volumes[ordinal as usize];
            submit_grid_cut_article(
                &mut pipeline,
                job_id,
                IndexPosition::First.volume_file_index(ordinal),
                segment_number,
                start as u64,
                &bytes[start..end],
                filename,
            )
            .await;
        }
    }
    (pipeline, working_dir, complete_dir)
}

/// How [`grid_fed_direct_job`] delivers the job.
#[derive(Debug, Clone, Copy, Default)]
struct GridFeed {
    /// Force the retained PAR2 session on, so the zero-I/O arm is reachable.
    retained_session: bool,
    /// Hold back the last volume's last article, which keeps the set **live**:
    /// a set whose volumes all completed reaches its verdict inside the feed
    /// and has finalized (or repaired) by the time the test looks at it.
    withhold_last_article: bool,
}

/// Every block verdict one volume carries, or an empty map when it carries
/// none at all.
fn verdicts_for(
    pipeline: &Pipeline,
    job_id: JobId,
    ordinal: u32,
) -> std::collections::BTreeMap<u32, crate::pipeline::integrity::BlockVerdict> {
    pipeline
        .block_crc_verdicts(NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn direct_routed_articles_claim_their_blocks_in_the_dual_crc_grid() {
    // The wiring itself. A direct volume's bytes never become a file, but they
    // are durable — in member partials and envelopes — before this seam records
    // anything, which is the same contract the conventional seam states. So the
    // grid may claim them, and every described slice of every volume has to come
    // back Intact with independent (pCRC-verified) coverage.
    //
    // The last volume's last article is withheld so the set is still **live**
    // when the verdicts are read: a set whose volumes all complete reaches its
    // PAR2 verdict inside the feed and finalizes, and finalization takes the
    // virtual volume image apart and retires the claims with it.
    let member_name = "Silver.Horizon.S03E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41401);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "non-vacuity: the set has to have stayed direct, or these verdicts came \
         off the conventional path; got {sets}"
    );
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");

    let complete_volumes = volumes.len() as u32 - 1;
    assert!(
        complete_volumes > 1,
        "non-vacuity: more than one volume has to have completed, or this proves \
         nothing about a set"
    );
    for ordinal in 0..complete_volumes {
        let verdicts = verdicts_for(&pipeline, job_id, ordinal);
        assert!(
            !verdicts.is_empty(),
            "volume {ordinal} produced no block verdict at all — the direct seam \
             never reached the grid; sets = {sets}"
        );
        assert!(
            verdicts.values().all(|verdict| matches!(
                verdict,
                crate::pipeline::integrity::BlockVerdict::Intact {
                    independently_covered: true
                }
            )),
            "volume {ordinal} claimed a block without independent coverage or with \
             damage: {verdicts:?}"
        );

        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let grid_match = pipeline.in_stream_verified_par2_match(file_id, &par2_set);
        assert!(
            grid_match.is_some(),
            "and the volume must bind to its description on the grid alone — every \
             described slice Intact at exactly the described length; verdicts = \
             {verdicts:?}"
        );
        let described = par2_set
            .file_description(&grid_match.expect("bound above").0)
            .expect("the description the binding named")
            .length;
        assert_eq!(
            verdicts.len() as u32,
            par2_set.slice_count_for_file(described),
            "every described slice must be claimed, not merely some of them"
        );
    }
}

#[tokio::test]
async fn a_direct_volumes_final_short_block_closes_on_its_decoded_length() {
    // The trap this exists for: the NZB's declared segment sizes are yEnc-
    // *encoded*, around 3% larger than the bytes that land, and closing the
    // short final block on that number puts its boundary past the described
    // extent — where `verdicts_against` refuses the comparison and the last
    // slice of every volume silently falls back to a read. The fixtures declare
    // inflated sizes precisely so this cannot pass by accident.
    let member_name = "Silver.Horizon.S03E02.mkv";
    let payload: Vec<u8> = (0..2350u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41402);
    // The last volume is held back for the same reason as above: finalization
    // retires the claims, and this test is about how they were made.
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");

    for ordinal in 0..volumes.len() as u32 - 1 {
        let decoded_len = volumes[ordinal as usize].1.len() as u64;
        assert_ne!(
            decoded_len % PAR2_SLICE_BYTES,
            0,
            "non-vacuity: volume {ordinal} has to end mid-block, or there is no \
             short final block to close"
        );
        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let declared = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .expect("the volume's assembly")
            .total_bytes();
        assert!(
            declared > decoded_len,
            "non-vacuity: the fixture must declare yEnc-encoded sizes, or the two \
             lengths agree and the trap cannot fire; declared={declared} \
             decoded={decoded_len}"
        );

        let last_block = u32::try_from((decoded_len - 1) / PAR2_SLICE_BYTES).unwrap();
        let verdicts = verdicts_for(&pipeline, job_id, ordinal);
        assert_eq!(
            verdicts.get(&last_block),
            Some(&crate::pipeline::integrity::BlockVerdict::Intact {
                independently_covered: true
            }),
            "volume {ordinal}'s final short block must close against the DECODED \
             length; verdicts = {verdicts:?}"
        );
        assert_eq!(
            verdicts.len() as u32,
            par2_set.slice_count_for_file(decoded_len),
            "and it must be the last of a complete claim, not an isolated one"
        );
    }
}

#[tokio::test]
async fn a_grid_verified_direct_set_takes_the_zero_io_session_pass() {
    // The prize. Every described slice of every volume was adjudicated in
    // stream, so the retained session — which reads no source bytes at all, its
    // `analyze()` skipping the scan because the volumes are absent from the
    // directory by construction — reports the verdict from evidence alone.
    // Before the seam fed the grid this arm was unreachable by construction.
    let member_name = "Silver.Horizon.S03E03.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41403);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            retained_session: true,
            ..GridFeed::default()
        },
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline.direct_session_pass_calls > 0,
        "the session arm must have answered for this set; sets = {sets}"
    );
    assert!(
        pipeline.direct_verify_read_splits.is_empty(),
        "and no read-and-verify pass may have run at all — that pass reads every \
         volume back through the virtual-volume adapter, which is the I/O this \
         whole seam exists to avoid; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        sets.contains("Finalized"),
        "and the verdict has to have cleared the set, or the zero-I/O pass \
         concluded something the job could not act on; got {sets}"
    );
}

#[tokio::test]
async fn a_damaged_direct_set_reads_only_the_volume_the_grid_could_not_claim() {
    // The damaged-set prize. One volume's envelope is corrupt, so the
    // all-or-nothing session gate refuses and the read-and-verify pass runs —
    // but re-reading the two volumes the decode pass already proved clean is
    // pure cost. They are stood in for on the same bar the session demands, and
    // only the damaged one is read.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E04.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41404);
    let (mut pipeline, _, complete_dir) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    assert_eq!(
        pipeline.direct_verify_read_splits.first().copied(),
        Some((2, 1)),
        "the pass that reached the damage verdict had to stand in for the two \
         clean volumes and read only the damaged one. (3, 0) would mean the grid \
         claimed damage it cannot see; (0, 3) would mean the direct seam never \
         reached the grid at all. splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        pipeline.direct_session_pass_calls == 0,
        "and the zero-I/O session arm must refuse a set it can only half claim"
    );

    // The job still ends where it always did: repaired in place, member
    // extracted, no volume file ever written.
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "the set must repair in place rather than hand its volumes back; got {sets}"
    );
    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let member = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| staging_member(&complete_dir, member_name));
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "and it must still reach the member the conventional gate produces; \
         sets = {sets}"
    );
}

/// Drives a job that has already been fed to whatever terminal state it
/// reaches, in the shape the repairable gate uses.
async fn drive_grid_fed_job_to_terminal(pipeline: &mut Pipeline, job_id: JobId) {
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(pipeline).await;
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(pipeline).await;
        settle_inflight_moves(pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(pipeline).await;
            settle_inflight_moves(pipeline).await;
        }
    }
}

#[tokio::test]
async fn demoting_a_direct_set_forgets_the_grid_state_its_volumes_carried() {
    // A demotion hands the volumes back to the conventional path, which is
    // about to fill real files with them — reconstructed from the routed bytes
    // or refetched off the wire. The direct phase's claims describe a different
    // image of the same coordinates, and merging the two would let a block
    // closed over one adjudicate bytes of the other.
    let member_name = "Silver.Horizon.S03E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41405);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let volume_file = NzbFileId {
        job_id,
        file_index: IndexPosition::First.volume_file_index(0),
    };
    let claimed_before = verdicts_for(&pipeline, job_id, 0);
    assert!(
        claimed_before.len() > 1,
        "non-vacuity: the volume has to carry claims across more than one \
         article, or 'the untouched half kept nothing' proves nothing; got \
         {claimed_before:?}"
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded)
        .await;

    assert!(
        verdicts_for(&pipeline, job_id, 0).is_empty(),
        "the demotion must retire every claim the direct phase made about this \
         volume"
    );

    // One article comes back conventionally, carrying bytes that are NOT what
    // the recovery set describes. Only the blocks it tiles may have a verdict at
    // all, and they must read Damaged: anything the direct phase claimed about
    // the rest would be a verdict about an image that no longer exists.
    let (start, end) = grid_article_extent(&volumes, 0, 0);
    let (filename, bytes) = &volumes[0];
    let rewritten: Vec<u8> = bytes[start..end].iter().map(|byte| !byte).collect();
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        volume_file.file_index,
        0,
        start as u64,
        &rewritten,
        filename,
    )
    .await;

    let after = verdicts_for(&pipeline, job_id, 0);
    assert!(
        !after.is_empty(),
        "non-vacuity: the conventional re-feed has to reach the grid, or the \
         assertion below is satisfied by an empty map"
    );
    assert!(
        after
            .values()
            .all(|verdict| matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)),
        "every verdict must come from the bytes the conventional path just \
         wrote: {after:?}"
    );
    assert!(
        after.len() < claimed_before.len(),
        "and the articles that did not come back must carry no verdict at all — \
         a surviving direct-phase claim is exactly what the forget prevents; \
         before={claimed_before:?} after={after:?}"
    );
}

#[tokio::test]
async fn a_replayed_direct_article_re_adjudicates_the_range_it_rewrote() {
    // The grid is positional, not sequential, so a duplicate is fed through the
    // direct seam on purpose — exactly as the conventional seam feeds one. The
    // replay rewrote its range on disk, and a verdict derived before it can only
    // stand if it is derived again from the arrival that did the rewriting.
    let member_name = "Silver.Horizon.S03E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41406);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let before = verdicts_for(&pipeline, job_id, 0);
    let derived_before = pipeline.block_crcs.blocks_derived();
    let (start, end) = grid_article_extent(&volumes, 0, 0);
    let first_block = u32::try_from(start as u64 / PAR2_SLICE_BYTES).unwrap();
    let straddling_block = u32::try_from((end as u64 - 1) / PAR2_SLICE_BYTES).unwrap();
    assert!(
        first_block < straddling_block,
        "non-vacuity: the replayed article has to tile at least one whole block \
         AND end inside another, or 'by range' has no range to be about"
    );
    assert_ne!(
        end as u64 % PAR2_SLICE_BYTES,
        0,
        "non-vacuity: the article must end mid-block, or nothing straddles the \
         boundary"
    );
    assert!(
        before.len() as u32 > straddling_block + 1,
        "non-vacuity: there have to be blocks beyond the replayed range to \
         survive it; got {before:?}"
    );

    let (filename, bytes) = &volumes[0];
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        IndexPosition::First.volume_file_index(0),
        0,
        start as u64,
        &bytes[start..end],
        filename,
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "non-vacuity: a byte-identical replay must not demote the set, or this \
         measures the demotion rather than the feed; got {sets}"
    );
    assert!(
        pipeline.block_crcs.blocks_derived() > derived_before,
        "the duplicate must have reached the grid and closed its blocks again: a \
         seam that skipped the feed would leave the counter where it was, and \
         with it a claim nothing re-derived"
    );

    let after = verdicts_for(&pipeline, job_id, 0);
    for block in first_block..straddling_block {
        assert_eq!(
            after.get(&block),
            Some(&crate::pipeline::integrity::BlockVerdict::Intact {
                independently_covered: true
            }),
            "a block the replay tiles on its own re-derives from the arrival that \
             rewrote it; after = {after:?}"
        );
    }
    assert!(
        !after.contains_key(&straddling_block),
        "the block straddling the replay's end must go unclaimed: the other \
         half's contribution was retired when that block first closed, and a \
         retired observation cannot vouch for bytes a rewrite has since \
         touched; after = {after:?}"
    );
    for (block, verdict) in &before {
        if *block > straddling_block {
            assert_eq!(
                after.get(block),
                Some(verdict),
                "and a block outside the rewritten range must keep the claim it \
                 already had — the invalidation is by RANGE, not by file; \
                 after = {after:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Cross-device probe
//
// Every other test in this file puts the intermediate and complete directories
// under one `TempDir`, so they share a filesystem and `rename(2)` between them
// always succeeds. That hides the exact failure this subsystem cares about: on
// the ordinary deployment — intermediate on local disk, complete on a NAS — a
// publish rename across the two returns `EXDEV` and completion falls back to
// copying every byte a second time.
//
// These probes run only when `WEAVER_XDEV_INTERMEDIATE` and
// `WEAVER_XDEV_COMPLETE` name directories on two different filesystems, which is
// what the container harness provides (two `--tmpfs` mounts). They print a
// machine-readable evidence block — device and inode numbers at each stage — and
// assert the verdict named by `WEAVER_XDEV_EXPECT` (`rename` or `copy`), so the
// same source can be run against a tree that writes payload to the intermediate
// filesystem and one that writes it to the complete filesystem, and each is held
// to what it actually guarantees.
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod cross_device {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    /// One file the probe found, with the two numbers that answer everything:
    /// `dev` says which filesystem it is on, `ino` says whether a later file is
    /// the *same* file (a rename) or a new one (a copy).
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Found {
        path: PathBuf,
        dev: u64,
        ino: u64,
        len: u64,
    }

    fn roots() -> Option<(PathBuf, PathBuf)> {
        let intermediate = std::env::var_os("WEAVER_XDEV_INTERMEDIATE")?;
        let complete = std::env::var_os("WEAVER_XDEV_COMPLETE")?;
        Some((PathBuf::from(intermediate), PathBuf::from(complete)))
    }

    fn dev_of(path: &Path) -> u64 {
        std::fs::metadata(path)
            .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
            .dev()
    }

    /// Every regular file under `root`, deepest-first order irrelevant.
    fn walk(root: &Path) -> Vec<Found> {
        let mut out = Vec::new();
        let mut queue = vec![root.to_path_buf()];
        while let Some(dir) = queue.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                if file_type.is_dir() {
                    queue.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let Ok(metadata) = std::fs::metadata(&path) else {
                    continue;
                };
                out.push(Found {
                    dev: metadata.dev(),
                    ino: metadata.ino(),
                    len: metadata.len(),
                    path,
                });
            }
        }
        out.sort_by(|left, right| left.path.cmp(&right.path));
        out
    }

    fn matching(root: &Path, predicate: impl Fn(&str) -> bool) -> Vec<Found> {
        walk(root)
            .into_iter()
            .filter(|found| {
                found
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(&predicate)
            })
            .collect()
    }

    fn report(stage: &str, found: &[Found]) {
        if found.is_empty() {
            println!("XDEV {stage} <none>");
        }
        for entry in found {
            println!(
                "XDEV {stage} dev={} ino={} len={} path={}",
                entry.dev,
                entry.ino,
                entry.len,
                entry.path.display()
            );
        }
    }

    fn fresh(root: &Path, tag: &str) -> PathBuf {
        let dir = root.join(tag);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The headline: where the payload is born, and whether the publish is a
    /// rename or a byte copy.
    #[tokio::test]
    async fn direct_store_payload_and_publish_across_two_filesystems() {
        let Some((intermediate_root, complete_root)) = roots() else {
            println!("XDEV skipped: set WEAVER_XDEV_INTERMEDIATE and WEAVER_XDEV_COMPLETE");
            return;
        };
        let expected = std::env::var("WEAVER_XDEV_EXPECT").unwrap_or_else(|_| "rename".to_string());

        let intermediate_dir = fresh(&intermediate_root, "payload-intermediate");
        let complete_dir = fresh(&complete_root, "payload-complete");
        let data_dir = fresh(&intermediate_root, "payload-data");
        let intermediate_dev = dev_of(&intermediate_dir);
        let complete_dev = dev_of(&complete_dir);
        println!("XDEV roots intermediate_dev={intermediate_dev} complete_dev={complete_dev}");
        assert_ne!(
            intermediate_dev, complete_dev,
            "the harness must mount the two roots on different filesystems, or this probe \
             proves nothing"
        );

        let member_name = "Silver.Horizon.S09E01.mkv";
        let payload: Vec<u8> = (0..24_000u32).map(|index| (index % 251) as u8).collect();
        let volumes = single_member_store_set(member_name, &payload, 3);

        let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
            data_dir,
            intermediate_dir.clone(),
            complete_dir.clone(),
            intermediate_dir.join("weaver.db"),
            BufferPoolConfig {
                small_count: 8,
                medium_count: 4,
                large_count: 2,
            },
            0,
            None,
        )
        .await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        // Small enough that held payload pages out to the scratch file, so the
        // working-data half of the split is observable too.
        pipeline.direct_store.set_holds_budget(64);

        let job_id = JobId(49001);
        let spec = direct_store_job_spec("Silver Horizon", &volumes);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        println!("XDEV working_dir path={}", working_dir.display());

        // Payload before the header on volume 0, so something is held and paged.
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

        // (1) Where is the payload while the job is still downloading?
        let mid_intermediate =
            matching(&intermediate_dir, |name| name.ends_with(".direct.partial"));
        let mid_complete = matching(&complete_dir, |name| name.ends_with(".direct.partial"));
        report("partial-in-intermediate", &mid_intermediate);
        report("partial-in-complete", &mid_complete);

        // (2) Working data must be on the intermediate filesystem either way.
        let scratch = matching(&intermediate_dir, |name| name.starts_with(".weaver-holds."));
        let envelopes = matching(&intermediate_dir, |name| name.ends_with(".envelope"));
        report("holds-scratch", &scratch);
        report("envelopes", &envelopes);
        assert!(
            !scratch.is_empty(),
            "non-vacuity: the holds budget should have forced a scratch file"
        );
        for entry in scratch.iter().chain(envelopes.iter()) {
            assert_eq!(
                entry.dev,
                intermediate_dev,
                "working data must stay on the intermediate filesystem: {}",
                entry.path.display()
            );
        }
        assert!(
            matching(&complete_dir, |name| name.starts_with(".weaver-holds.")
                || name.ends_with(".envelope"))
            .is_empty(),
            "no working data may be written to the complete filesystem"
        );

        let partials: Vec<Found> = mid_intermediate
            .iter()
            .chain(mid_complete.iter())
            .cloned()
            .collect();
        assert!(
            !partials.is_empty(),
            "non-vacuity: nothing routed into a member partial"
        );
        let born_dev = partials[0].dev;
        println!(
            "XDEV verdict payload-born-on={}",
            if born_dev == complete_dev {
                "complete"
            } else {
                "intermediate"
            }
        );

        // Finish the download; the set finalizes and commits its member.
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            if (file_index, segment_number) == (0, 0) || (file_index, segment_number) == (0, 1) {
                continue;
            }
            submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number)
                .await;
        }
        drain_rar_refreshes(&mut pipeline).await;
        let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
        assert!(
            shape.contains("Finalized"),
            "the set must finalize, got {shape}"
        );

        // (3) The committed member, immediately before completion publishes it.
        let committed: Vec<Found> = walk(&intermediate_dir)
            .into_iter()
            .chain(walk(&complete_dir))
            .filter(|found| {
                found
                    .path
                    .file_name()
                    .is_some_and(|name| name == member_name)
            })
            .collect();
        report("member-after-commit", &committed);
        assert_eq!(
            committed.len(),
            1,
            "the member must exist in exactly one place before the move"
        );
        let before_move = committed[0].clone();
        assert_eq!(
            before_move.len,
            payload.len() as u64,
            "and hold the whole member"
        );

        drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

        // (4) The published member.
        let published: Vec<Found> = walk(&complete_dir)
            .into_iter()
            .filter(|found| {
                found
                    .path
                    .file_name()
                    .is_some_and(|name| name == member_name)
            })
            .collect();
        report("member-after-publish", &published);
        assert_eq!(
            published.len(),
            1,
            "the job must publish exactly one member"
        );
        let after_move = published[0].clone();
        assert_eq!(
            std::fs::read(&after_move.path).unwrap(),
            payload,
            "and it must be byte-correct"
        );
        assert_eq!(
            after_move.dev, complete_dev,
            "the published member is on the complete filesystem by definition"
        );

        // The verdict, and it needs no instrumentation: a rename keeps the
        // inode, a copy cannot.
        let verdict = if after_move.dev == before_move.dev && after_move.ino == before_move.ino {
            "rename"
        } else {
            "copy"
        };
        println!(
            "XDEV verdict publish={verdict} before=(dev={},ino={}) after=(dev={},ino={})",
            before_move.dev, before_move.ino, after_move.dev, after_move.ino
        );

        // (5) Nothing of the payload may be left on the intermediate filesystem.
        let leftovers = walk(&intermediate_dir)
            .into_iter()
            .filter(|found| {
                found
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == member_name || name.ends_with(".direct.partial"))
            })
            .collect::<Vec<_>>();
        report("payload-left-in-intermediate", &leftovers);
        assert!(
            leftovers.is_empty(),
            "no payload may survive on the intermediate filesystem"
        );

        assert_eq!(
            verdict, expected,
            "publish verdict; set WEAVER_XDEV_EXPECT to the behaviour this tree guarantees"
        );
    }

    /// The failure path: a cancelled job leaves nothing behind on the complete
    /// filesystem.
    #[tokio::test]
    async fn a_cancelled_job_cleans_what_it_wrote_on_the_complete_filesystem() {
        let Some((intermediate_root, complete_root)) = roots() else {
            println!("XDEV skipped: set WEAVER_XDEV_INTERMEDIATE and WEAVER_XDEV_COMPLETE");
            return;
        };

        let intermediate_dir = fresh(&intermediate_root, "cancel-intermediate");
        let complete_dir = fresh(&complete_root, "cancel-complete");
        let data_dir = fresh(&intermediate_root, "cancel-data");
        assert_ne!(dev_of(&intermediate_dir), dev_of(&complete_dir));

        let member_name = "Silver.Horizon.S09E02.mkv";
        let payload: Vec<u8> = (0..24_000u32).map(|index| (index % 241) as u8).collect();
        let volumes = single_member_store_set(member_name, &payload, 3);

        let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
            data_dir,
            intermediate_dir.clone(),
            complete_dir.clone(),
            intermediate_dir.join("weaver.db"),
            BufferPoolConfig {
                small_count: 8,
                medium_count: 4,
                large_count: 2,
            },
            0,
            None,
        )
        .await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

        let job_id = JobId(49002);
        let spec = direct_store_job_spec("Silver Horizon", &volumes);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        // Mid-store: routed, not finished.
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
        let staging = complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string());
        println!(
            "XDEV cancel staging_exists_before={} path={}",
            staging.exists(),
            staging.display()
        );
        report("cancel-before-complete", &walk(&complete_dir));
        report("cancel-before-intermediate", &walk(&intermediate_dir));

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        pipeline
            .handle_command(SchedulerCommand::CancelJob {
                job_id,
                origin: crate::jobs::handle::CancellationOrigin::User,
                reply: reply_tx,
            })
            .await;
        reply_rx.await.unwrap().unwrap();

        // The cleanup is spawned; give it a bounded window to land.
        for _ in 0..200 {
            if !staging.exists() && !working_dir.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        report("cancel-after-complete", &walk(&complete_dir));
        report("cancel-after-intermediate", &walk(&intermediate_dir));
        println!(
            "XDEV cancel staging_exists_after={} working_dir_exists_after={}",
            staging.exists(),
            working_dir.exists()
        );
        assert!(
            !staging.exists(),
            "a cancelled job must not leave its staging directory on the complete filesystem"
        );
        assert!(
            walk(&complete_dir).is_empty(),
            "and must leave no bytes there at all: {:?}",
            walk(&complete_dir)
        );
        assert!(
            !working_dir.exists(),
            "nor its working directory on the intermediate filesystem"
        );
    }
}

#[tokio::test]
async fn a_uuencoded_article_demotes_the_direct_set_that_claims_its_volume() {
    // Sets are admitted from the NZB's filenames, before a single article has
    // been decoded, so an archive posted in uuencode is admitted exactly like a
    // yEnc one — and can never be routed, because a uuencode article declares
    // no offset to route on. Excluding those articles quietly is not enough: a
    // starved set neither finalizes nor demotes, its volumes keep answering
    // `is_direct_source_file`, and that suppresses the archive probe which
    // dispatches extraction. The job would complete with its archive sitting
    // unextracted on disk.
    let member_name = "Silver.Horizon.S01E17.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41099);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Admission is lazy, so the very first article of the job is uuencode and
    // the set has not been built yet — which is exactly the shape that must
    // still end with a demoted set rather than one admitted moments later.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    super::decode_and_files::submit_uu_segment_named(
        &mut pipeline,
        file_id,
        0,
        &volumes[0].1[..64],
        false,
        false,
        member_name,
    )
    .await;

    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the set was admitted"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| set.is_demoted()),
        "a uuencode article must take the set off the direct path"
    );
    assert!(
        !pipeline.is_direct_source_file(file_id),
        "the volume is back on the conventional path, so extraction can be dispatched for it"
    );
}

/// Absolute path of one source volume's sparse envelope, found by suffix.
///
/// The name is built from the set's plan, so a test that hard-coded it would
/// pin a private naming scheme rather than the behaviour under test.
fn envelope_path_for_volume(pipeline: &Pipeline, job_id: JobId, volume_index: u32) -> PathBuf {
    pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .find_map(|set| {
            set.plan()
                .volumes
                .contains_key(&volume_index)
                .then(|| set.plan().envelope_path(volume_index))
        })
        .expect("the set owns this volume")
}

#[tokio::test]
async fn the_post_direct_repair_pass_reads_only_what_the_repair_rewrote() {
    // Before a repair, the quiet direct pass stands in for every volume the
    // dual-CRC grid adjudicated and reads only the rest — that is the whole
    // point of the grid and it is measured by the sibling test.
    //
    // After a repair, it stands in for nothing FROM THE WIRE — the grid and
    // the session are both skipped, unconditionally, for the reasons this
    // whole seam exists (see `verify_direct_sets_quietly`'s docs). But it is
    // no longer required to read every volume either: the repair carries its
    // own pre-repair verdict forward, and every volume that verdict already
    // called `Complete` was proven by a DISK read minutes ago in this same
    // flow, not by wire evidence — so only the volume(s) the repair actually
    // rewrote need reading again. This is the direct-store mirror of what
    // `verify_repaired_par2_files_with_placement` already does for a
    // conventional set.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E09.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41410);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    assert_eq!(
        pipeline.direct_verify_read_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: the PRE-repair pass must have claimed the two clean \
         volumes and read only the one damaged one, or there is nothing for \
         the repair's write set to be narrower than; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        pipeline.direct_post_repair_read_splits.is_empty(),
        "and nothing may be recorded as post-repair before a repair has run"
    );

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    assert!(
        !pipeline.direct_post_repair_read_splits.is_empty(),
        "a repair ran, so at least one pass must have been a read-back; all \
         splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &pipeline.direct_post_repair_read_splits {
        assert_eq!(
            *claimed, 0,
            "a post-repair pass may stand in for nothing FROM THE WIRE; \
             splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
        assert_eq!(
            *read, 1,
            "and with a live carry it must read only the one volume the \
             repair actually rewrote, not the two the pre-repair pass had \
             already proven from disk; splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
    }
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "the point of carrying the pre-repair verdict forward is that the \
         completion gate settles it directly instead of asking \
         `verify_par2_with_placement` to read this virtual set again to reach \
         the same answer — a whole-set pass here would be exactly the second \
         read this design exists to remove"
    );
}

#[tokio::test]
async fn a_post_repair_pass_without_a_surviving_carry_still_reads_every_volume() {
    // The fallback this whole design depends on staying reachable: a carry
    // can go missing — the job restarted, the pipeline evicted it, the
    // recovery set got rebound — and when it does, the post-repair pass has
    // nothing narrower to trust than the full, unconditional read it has
    // always taken. Proven here by tearing the carry out from under a repair
    // that just ran, before calling the gate again to force the read-back
    // that would otherwise have consulted it.
    //
    // Driven through direct, single-step calls to
    // `resolve_direct_sets_before_par2_repairer` — the same shape the ticket
    // liveness tests already use — rather than the full completion-check
    // drive loop, because that loop's own queue pumping can settle the ticket
    // before a test gets a chance to evict anything between the repair and
    // the read-back it leaves behind.
    let member_name = "Silver.Horizon.S01E30.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41432);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or there is \
         no carry for this test to evict"
    );
    assert!(
        !pipeline.direct_post_repair_carry.is_empty(),
        "non-vacuity: the repair must have left a carry behind for this test \
         to evict"
    );
    pipeline.direct_post_repair_carry.clear();

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let resolution = loop {
        let resolution = pipeline
            .resolve_direct_sets_before_par2_repairer(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone(),
            )
            .await;
        if !matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ) {
            break resolution;
        }
        let done = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.direct_post_repair_done_rx.recv(),
        )
        .await
        .expect("the post-repair read-back should finish")
        .expect("the post-repair completion channel stays open");
        pipeline.handle_direct_post_repair_done(done);
    };

    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Clean(_)
        ),
        "with no fresh damage the read-back must settle the set clean; got \
         {resolution:?}"
    );
    assert!(
        !pipeline.direct_post_repair_read_splits.is_empty(),
        "a repair ran, so at least one pass must have been a read-back; all \
         splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &pipeline.direct_post_repair_read_splits {
        assert_eq!(
            *claimed, 0,
            "a post-repair pass may stand in for nothing FROM THE WIRE; \
             splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
        assert_eq!(
            *read, 3,
            "and without a surviving carry it must fall back to reading every \
             described volume, exactly as it always did; splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
    }
}

#[tokio::test]
async fn a_disk_fault_in_the_repaired_volume_is_caught_after_a_repair() {
    // The half of the safety property a selective post-repair read-back keeps:
    // a fault under the volume the repair itself just rewrote is still caught,
    // because that volume is exactly what the write set names — carried or
    // not, it is never one of the files a post-repair pass stands in for.
    //
    // Introduced *between* the repair landing and the read-back that follows
    // it, so this is not the repair's own accounting catching its own
    // mistake: it is a second, independent fault — a bad sector, a stray
    // write from something else entirely — landing on bytes the repair had
    // already got right.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E10.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    // Volume 1's recovery record is damaged on the WIRE, which is what gives
    // the job a repair to run — and makes volume 1 the write set.
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41411);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    let pre_repair_splits = pipeline.direct_verify_read_splits.clone();
    assert_eq!(
        pre_repair_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: volume 1 has to have been the one READ (and found \
         damaged) pre-repair, or it is not the write set the fault below is \
         supposed to land inside; splits = {pre_repair_splits:?}"
    );

    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    let mut faulted = false;
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        // The instant the repair has left its carry behind — after it ran,
        // before the read-back that consults it — land the fault on the
        // volume the carry's write set names, and never again.
        if !faulted
            && pipeline
                .direct_post_repair_carry
                .get(&job_id)
                .is_some_and(|carry| !carry.write_set.is_empty())
        {
            let envelope = envelope_path_for_volume(&pipeline, job_id, 1);
            let mut bytes = std::fs::read(&envelope).expect("volume 1's envelope exists on disk");
            assert!(
                bytes.len() > 64,
                "non-vacuity: the envelope must actually hold bytes to corrupt; len = {}",
                bytes.len()
            );
            for byte in bytes.iter_mut().take(64) {
                *byte ^= 0xFF;
            }
            std::fs::write(&envelope, &bytes).expect("the fault lands on disk");
            faulted = true;
        }
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
    }

    assert!(
        faulted,
        "non-vacuity: the repair must actually have left a carry to land the \
         fault beside, or this test proves nothing"
    );

    let post_repair = pipeline.direct_post_repair_read_splits.clone();
    assert!(
        !post_repair.is_empty(),
        "a repair ran, so a read-back pass must have followed it; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        post_repair
            .iter()
            .all(|(claimed, read)| *claimed == 0 && *read >= 1),
        "and every read-back lap must at least have read volume 1 back; \
         splits = {post_repair:?}"
    );

    // The verdict the last quiet pass reached. Volume 1 is the file the fault
    // landed on, and it must not come back Complete.
    let verdict = pipeline
        .last_direct_verdict
        .clone()
        .expect("a quiet pass recorded its verdict");
    let volume_one = &clean[1].0;
    let entry = verdict
        .files
        .iter()
        .find(|file| file.filename == *volume_one)
        .unwrap_or_else(|| panic!("volume 1 is described; verdict = {verdict:?}"));
    assert!(
        !matches!(entry.status, par2_rs::verify::FileStatus::Complete),
        "the post-repair pass reads back every volume in the write set, and \
         volume 1 is in it whether the carry survives or not — a `Complete` \
         verdict here is a corrupt member shipping in a finished job. \
         status = {:?}",
        entry.status
    );
}

#[tokio::test]
async fn a_disk_fault_outside_the_write_set_is_the_accepted_residual_after_a_repair() {
    // The trade-off this whole redesign makes, pinned rather than left
    // implicit: a selective post-repair pass reads back the write set and
    // nothing else, so a fault landing on a volume the repair did NOT rewrite
    // — this one was grid-claimed pre-repair and never touched a disk read at
    // all — ships unnoticed. This is not a bug this pass forgot to close; it
    // is the same trust class `verify_repaired_par2_files_with_placement`
    // already accepts for a conventional set's untouched files, extended here
    // to the direct-store path for the same reason: re-reading bytes the
    // repair never wrote answers a question a very recent pass already
    // answered, and a knob to force the wider read would only buy back the
    // cost this redesign exists to remove.
    //
    // If this test ever starts failing because the fault gets caught, that is
    // a sign the selective read-back widened again — worth knowing, and worth
    // deciding on purpose rather than by accident.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E13.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    // Volume 1's recovery record is damaged on the WIRE, which is what gives
    // the job a repair to run at all. Volume 0 is perfect on the wire and is
    // never in the write set.
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41431);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    let pre_repair_splits = pipeline.direct_verify_read_splits.clone();
    assert_eq!(
        pre_repair_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: volume 0 has to have been CLAIMED by the grid rather \
         than read, or it was never a candidate for the write set to leave \
         behind; splits = {pre_repair_splits:?}"
    );

    // The fault. Volume 0's envelope is rewritten on disk with bytes the wire
    // never carried — a bad sector, a short write, a neighbour scribbling. The
    // grid's claim for volume 0 still stands: it was made from article CRCs
    // recorded when those bytes were durable, and nothing has re-read them
    // since — and after the repair runs, nothing will, because volume 0 is
    // never in its write set.
    let envelope = envelope_path_for_volume(&pipeline, job_id, 0);
    let mut faulted = std::fs::read(&envelope).expect("volume 0's envelope exists on disk");
    assert!(
        faulted.len() > 64,
        "non-vacuity: the envelope must actually hold bytes to corrupt; len = {}",
        faulted.len()
    );
    for byte in faulted.iter_mut().take(64) {
        *byte ^= 0xFF;
    }
    std::fs::write(&envelope, &faulted).expect("the fault lands on disk");

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    let post_repair = pipeline.direct_post_repair_read_splits.clone();
    assert!(
        !post_repair.is_empty(),
        "a repair ran, so a read-back pass must have followed it; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &post_repair {
        assert_eq!(*claimed, 0, "splits = {post_repair:?}");
        assert_eq!(
            *read, 1,
            "a live carry narrows the read-back to the one volume the repair \
             actually rewrote — volume 0 is not it; splits = {post_repair:?}"
        );
    }

    // The verdict the last quiet pass reached. Volume 0 was never read back,
    // so its carried pre-repair verdict — `Complete`, from the grid — stands.
    let verdict = pipeline
        .last_direct_verdict
        .clone()
        .expect("a quiet pass recorded its verdict");
    let volume_zero = &clean[0].0;
    let entry = verdict
        .files
        .iter()
        .find(|file| file.filename == *volume_zero)
        .unwrap_or_else(|| panic!("volume 0 is described; verdict = {verdict:?}"));
    assert!(
        matches!(entry.status, par2_rs::verify::FileStatus::Complete),
        "this is the accepted residual, not a surprise: a fault outside the \
         write set does not get caught, because nothing reads that volume \
         back post-repair when a carry survives. status = {:?}",
        entry.status
    );
}

#[tokio::test]
async fn a_clean_direct_verdict_settles_without_a_second_whole_set_pass() {
    // The other half of the redesign: a direct set that was never damaged
    // reaches `DirectPar2Resolution::Clean` from the completion gate's own
    // direct-aware seam, and that verdict now settles the job directly
    // instead of being thrown away and re-derived by asking
    // `verify_par2_with_placement` to read the same virtual volumes again.
    // Proven two ways: the whole-set authoritative counter never moves, and
    // the job's verification-complete event fires exactly once — not zero
    // (the gate must still announce a verdict) and not twice (which is what
    // the discarded-verdict bug produced: one from the direct gate's own
    // settle, a second from the redundant whole-set pass it used to fall
    // through to).
    let member_name = "Silver.Horizon.S03E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41440);
    // The last article is withheld so the set is still live — and has not
    // yet reached a verdict — when the feed returns, which is what lets this
    // test subscribe before the one verification event it means to count.
    // `GridFeed::default()` reaches the verdict *inside* the feed itself, too
    // early for a subscription taken after it to see anything.
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let mut events = pipeline.event_tx.subscribe();

    // The withheld article: the last volume's last segment, delivered now
    // that the subscription is in place.
    let last_ordinal = volumes.len() as u32 - 1;
    let last_segment = GRID_ARTICLES as u32 - 1;
    let (start, end) = grid_article_extent(&volumes, last_ordinal, last_segment);
    let (filename, bytes) = &volumes[last_ordinal as usize];
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        IndexPosition::First.volume_file_index(last_ordinal),
        last_segment,
        start as u64,
        &bytes[start..end],
        filename,
    )
    .await;

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "non-vacuity: the job must actually finish, or the assertions below \
         prove nothing; status = {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "an undamaged direct set's verdict must be settled from the direct \
         gate's own read, never from a whole-set `verify_par2_with_placement` \
         pass over the same virtual volumes"
    );

    let announced = drain_job_events(&mut events, job_id);
    let verification_complete: Vec<_> = announced
        .iter()
        .filter(|event| matches!(event, PipelineEvent::JobVerificationComplete { .. }))
        .collect();
    assert_eq!(
        verification_complete.len(),
        1,
        "exactly one verdict must be announced — the discarded-verdict bug \
         this design fixes produced a second one from the redundant whole-set \
         pass; got {announced:?}"
    );
    assert!(
        matches!(
            verification_complete[0],
            PipelineEvent::JobVerificationComplete { passed: true, .. }
        ),
        "and it must report the clean verdict the set actually had; got \
         {:?}",
        verification_complete[0]
    );
}

#[tokio::test]
async fn demoting_a_carrying_set_clears_the_carry_and_the_ticket_slots() {
    // A demoted set's volumes become real files and hand off to the
    // conventional repairer, which brings its own post-repair pass — so any
    // post-repair bookkeeping this job was carrying for the direct gate must
    // not survive to describe bytes a different repair path now owns. Left
    // behind, a stale carry would (at best) be silently ignored by the
    // recovery-set-id check and (at worst, if the demoted set later comes
    // back on the same recovery set) stand in for volumes a conventional
    // repair just rewrote.
    let member_name = "Silver.Horizon.S01E31.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41433);
    let (mut pipeline, _working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or there is \
         no carry for the demotion below to clear"
    );
    assert!(
        !pipeline.direct_post_repair_carry.is_empty(),
        "non-vacuity: the repair must have left a carry behind"
    );

    // A fabricated in-flight ticket and a parked result, standing in for
    // whatever bookkeeping a real post-repair read-back would have left mid
    // flight. The demotion must clear these too, not only the carry — a
    // lingering in-flight entry is what used to suppress the drain-tick
    // re-arm in `schedule_job_completion_check_if_download_pipeline_drained`.
    let recovery_set_id = pipeline
        .par2_set(job_id)
        .map(|set| set.recovery_set_id)
        .expect("the index parsed");
    pipeline.direct_post_repair_in_flight.insert(
        job_id,
        crate::pipeline::DirectPostRepairWork {
            work_id: 1,
            recovery_set_id,
            submitted_at: std::time::Instant::now(),
        },
    );
    pipeline.direct_post_repair_results.insert(
        job_id,
        (
            recovery_set_id,
            Ok(par2_rs::VerificationResult {
                files: Vec::new(),
                recovery_blocks_available: 0,
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            }),
        ),
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::UnparsableVolume)
        .await;

    assert!(
        pipeline.direct_post_repair_carry.is_empty(),
        "the demotion must clear this job's post-repair carry"
    );
    assert!(
        pipeline.direct_post_repair_in_flight.is_empty(),
        "and its in-flight ticket bookkeeping"
    );
    assert!(
        pipeline.direct_post_repair_results.is_empty(),
        "and any result parked waiting for a gate lap to consume it"
    );
}

#[tokio::test]
async fn a_ticket_parked_against_a_stale_recovery_set_is_dropped_for_a_fresh_one() {
    // The permanent-park bug this fix closes: before it, an in-flight ticket
    // whose `recovery_set_id` disagreed with the set currently being resolved
    // returned `None` forever — no new ticket ever started, so no result ever
    // arrived, so nothing ever re-armed the job. `recovery_set_id` can
    // legitimately drift out from under an in-flight ticket (a later PAR2
    // index rebinds the served set), so the ticket has to be dropped and
    // replaced instead of parking behind it.
    let member_name = "Silver.Horizon.S01E32.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41434);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or the gate \
         below never reaches the ticket seam at all"
    );

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let stale_recovery_set_id = par2_rs::RecoverySetId::from_bytes([0xAA; 16]);
    assert_ne!(
        stale_recovery_set_id, par2_set.recovery_set_id,
        "non-vacuity: the fabricated id must actually disagree with the set \
         this job serves"
    );
    // A ticket parked against a recovery set this job no longer serves — the
    // shape a rebind leaves behind, fabricated directly rather than driving a
    // second PAR2 index through the harness to produce it.
    pipeline.direct_post_repair_in_flight.insert(
        job_id,
        crate::pipeline::DirectPostRepairWork {
            work_id: 999,
            recovery_set_id: stale_recovery_set_id,
            submitted_at: std::time::Instant::now(),
        },
    );

    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
        )
        .await;

    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ),
        "a fresh ticket must have started for the set this job actually \
         serves; got {resolution:?}"
    );
    let in_flight = pipeline
        .direct_post_repair_in_flight
        .get(&job_id)
        .expect("a fresh ticket must be in flight");
    assert_eq!(
        in_flight.recovery_set_id, par2_set.recovery_set_id,
        "the stale entry must have been replaced, not left in place — a \
         ticket still parked against the fabricated id would mean the park \
         was never broken"
    );
    assert_ne!(
        in_flight.work_id, 999,
        "the fresh ticket must carry a work id of its own, fencing the stale \
         one's done message if it ever lands"
    );

    // And the job actually finishes — the permanent-park bug's whole
    // signature was that nothing downstream of the stale entry ever ran
    // again.
    let done = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        pipeline.direct_post_repair_done_rx.recv(),
    )
    .await
    .expect("the fresh ticket's read-back should finish")
    .expect("the post-repair completion channel stays open");
    assert_eq!(
        done.recovery_set_id, par2_set.recovery_set_id,
        "the result that lands must be the fresh ticket's, not a stale one"
    );
    pipeline.handle_direct_post_repair_done(done);
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir)
        .await;
    assert!(
        !matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ),
        "with the fresh ticket's result in hand the gate must reach a verdict \
         rather than parking again; got {resolution:?}"
    );
}

#[tokio::test]
async fn the_post_repair_discriminator_follows_the_repair_latch() {
    // The read-back rule is only as good as the question "has a repair run?",
    // and the two tests above cannot pin that question on their own: the grid is
    // independently empty after a repair, so they pass either way (verified by
    // re-running them with the guard removed). This pins the discriminator.
    const RR_BYTES: usize = 512;
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();

    // A set nothing damaged. It verifies clean and never repairs, so every pass
    // it sees may stand in for what the grid adjudicated.
    let temp_dir = tempfile::tempdir().unwrap();
    let clean_job = JobId(41412);
    let undamaged = single_member_store_set("Silver.Horizon.S03E11.mkv", &payload, 3);
    let clean_par2 = par2_index_over_volumes(&undamaged);
    let (clean_pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        clean_job,
        &undamaged,
        &clean_par2,
        GridFeed::default(),
    )
    .await;
    assert!(
        !clean_pipeline.direct_sets_repaired_in_place(clean_job),
        "a set that was never repaired must not be read back as if it had been;          sets = {:?}",
        clean_pipeline.direct_store.sets_for(clean_job)
    );

    // And one whose recovery record arrived damaged, which repairs in place.
    let damaged_temp = tempfile::tempdir().unwrap();
    let damaged_job = JobId(41413);
    let clean_set = recovery_record_store_set("Amber.Trail.S03E11.mkv", &payload, 3, RR_BYTES);
    let damaged_par2 = repairable_par2_index(&clean_set, 4);
    let mut damaged = clean_set.clone();
    damage_recovery_record(&mut damaged, 1, RR_BYTES);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &damaged_temp,
        damaged_job,
        &damaged,
        &damaged_par2,
        GridFeed::default(),
    )
    .await;
    assert!(
        pipeline.direct_store.repair_attempts > 0,
        "non-vacuity: the fixture must actually repair in place, or the latch \
         never burns and this asserts nothing"
    );
    assert!(
        pipeline.direct_sets_repaired_in_place(damaged_job),
        "the latch burns at a repair's first irreversible step, so from that \
         moment on every quiet pass is a read-back"
    );

    // A demoted set is not this pass's business: its volumes became real files
    // and the conventional repairer brings its own post-repair verification.
    for index in 0..pipeline.direct_store.sets_for(damaged_job).len() {
        if let Some(set) = pipeline.direct_store.set_mut(damaged_job, index) {
            set.demote(crate::pipeline::direct_store::router::DemotionReason::UnparsableVolume);
        }
    }
    assert!(
        !pipeline.direct_sets_repaired_in_place(damaged_job),
        "a demoted set hands its volumes to the conventional path"
    );
}

#[tokio::test]
async fn an_obfuscated_direct_set_still_reaches_zero_io_grid_adjudication() {
    // An obfuscated post names its volumes after a hash while the recovery set
    // describes their real names. A volume that binds to no description
    // forfeits the dual-CRC grid entirely — nothing to measure a verdict
    // against — so every volume would be read back at completion.
    //
    // MEASURED, and it is not what the content binder was written for: this
    // shape already binds by NAME. PAR2-driven identity classification assigns
    // each volume a `canonical_filename` of "silver.horizon.partNN.rar" with
    // `classification_source: Par2`, and `resolve_par2_file_binding` searches
    // the canonical name along with the posted one. Verified by running this
    // test with the content fallback removed: it still passes.
    //
    // It stays as the regression pin for that path — the claim "an obfuscated
    // direct set gets zero-I/O adjudication" is worth holding however it is
    // achieved, and if the classifier ever stops inferring the name, the
    // content binder is what keeps this green.
    let member_name = "Silver.Horizon.S04E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let described = single_member_store_set(member_name, &payload, 3);
    // The recovery set describes the volumes under their real names...
    let par2_bytes = par2_index_over_volumes(&described);
    // ...while the post carries them under an obfuscated stem.
    let posted: Vec<(String, Vec<u8>)> = described
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("a7f3e91c8b2d4f60.part{:02}.rar", index + 1),
                bytes.clone(),
            )
        })
        .collect();
    assert!(
        posted
            .iter()
            .zip(described.iter())
            .all(|((posted, _), (real, _))| posted != real),
        "non-vacuity: the posted names must differ from the described ones"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41420);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &posted,
        &par2_bytes,
        GridFeed {
            retained_session: true,
            ..GridFeed::default()
        },
    )
    .await;

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    for ordinal in 0..posted.len() as u32 {
        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let bound = pipeline
            .resolve_par2_file_binding(file_id)
            .unwrap_or_else(|| panic!("volume {ordinal} must bind to a description"));
        let described_name = &par2_set
            .file_description(&bound.par2_file_id)
            .expect("the bound description")
            .filename;
        assert_eq!(
            described_name, &described[ordinal as usize].0,
            "volume {ordinal} must bind to the description holding its own bytes, \
             not merely to some description"
        );
    }

    // And the binding reached the grid while it mattered. The observables have
    // to be records of what the pass DID rather than state inspected
    // afterwards: finalization retires a finalized set's grid entries on
    // purpose, so a post-hoc look at the collector finds nothing whether the
    // adjudication happened or not.
    assert!(
        pipeline.direct_session_pass_calls > 0,
        "the zero-I/O session arm answered, which it can only do when every \
         described volume is adjudicated in stream"
    );
    assert!(
        pipeline.direct_verify_read_splits.is_empty(),
        "so no volume was read back at all; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Finalized"),
        "and the verdict has to settle the aggregate gate and clear the set, or the \
         zero-I/O pass concluded something the job could not act on; got {sets}"
    );
}
