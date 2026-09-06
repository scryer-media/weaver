//! Direct-store routing.
//!
//! The spine is differential: the identical job gate is run with routing on and
//! off, and the outputs must be byte-identical. With routing on, no source
//! volume may ever appear on disk.

use super::*;

use std::path::Path;

use crate::pipeline::direct_store::DirectStoreGate;
use crate::pipeline::direct_store::router::{DemotionReason, VolumeDemand};
use crate::pipeline::direct_store::wiring::MAX_DIRECT_REPAIR_DEFER_WAVES;

use crate::pipeline::direct_store::barrier::BarrierDemand;

mod scenarios;

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
    quick_open_store_set_shaped(member_name, payload, forged_name, true)
}

/// The same fixture, with control over whether the `QO` block is closed by a
/// cached **end-of-archive** record.
///
/// `cached_end_record` is what decides whether the cache is used at all:
/// unrar's reader adopts a Quick Open list only once it has seen the end record
/// that proves the list is complete, and drops the whole cache otherwise. Real
/// archivers do not write one — a `QO` block caches file headers, and the end
/// header comes after it — so `false` is the shape found in the wild, where the
/// locator is present, the cache is read and rejected, and the physical walk
/// supplies the members. `true` is the shape the cross-check tests need, and
/// the only one in which a forged cache entry can reach anything at all.
fn quick_open_store_set_shaped(
    member_name: &str,
    payload: &[u8],
    forged_name: Option<&str>,
    cached_end_record: bool,
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
    if cached_end_record {
        records.extend_from_slice(&build_test_rar_qopen_record(
            QOPEN_OFFSET,
            QOPEN_OFFSET - 32,
            &build_test_rar_end_header(false),
        ));
    }
    second.extend_from_slice(&build_test_rar_service_header("QO", records.len() as u64));
    second.extend_from_slice(&records);

    vec![
        ("silver.horizon.part01.rar".to_string(), first),
        ("silver.horizon.part02.rar".to_string(), second),
    ]
}

/// A two-volume stored set whose **last** volume holds two members — the tail
/// of one split across both volumes, then a second one whole — under a locator
/// and an honest, end-record-closed `QO` cache past the end header.
///
/// Also returns the physical offset of the second member's file header, so a
/// test can place that header inside an article that has not arrived while the
/// article carrying the cache has.
fn quick_open_two_member_store_set(
    split_name: &str,
    split_payload: &[u8],
    whole_name: &str,
    whole_payload: &[u8],
) -> (Vec<(String, Vec<u8>)>, u64) {
    let split_crc = checksum::crc32(split_payload);
    let split = split_payload.len() / 2;

    let mut first = Vec::new();
    first.extend_from_slice(&TEST_RAR5_SIG);
    first.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    first.extend_from_slice(&build_test_rar_file_header(
        split_name,
        0x0010,
        split as u64,
        split_payload.len() as u64,
        Some(checksum::crc32(&split_payload[..split])),
    ));
    first.extend_from_slice(&split_payload[..split]);
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
    let split_header = build_test_rar_file_header(
        split_name,
        0x0008,
        (split_payload.len() - split) as u64,
        split_payload.len() as u64,
        Some(split_crc),
    );
    let whole_header = build_test_rar_file_header(
        whole_name,
        0,
        whole_payload.len() as u64,
        whole_payload.len() as u64,
        Some(checksum::crc32(whole_payload)),
    );
    let split_header_offset = (TEST_RAR5_SIG.len() + main.len()) as u64;
    let whole_header_offset =
        split_header_offset + split_header.len() as u64 + (split_payload.len() - split) as u64;

    let mut second = Vec::new();
    second.extend_from_slice(&TEST_RAR5_SIG);
    second.extend_from_slice(&main);
    second.extend_from_slice(&split_header);
    second.extend_from_slice(&split_payload[split..]);
    second.extend_from_slice(&whole_header);
    second.extend_from_slice(whole_payload);
    second.extend_from_slice(&build_test_rar_end_header(false));
    assert!(
        second.len() as u64 <= QOPEN_OFFSET,
        "the physical headers must end before the QO block"
    );
    second.resize(QOPEN_OFFSET as usize, 0);

    let mut records = build_test_rar_qopen_record(QOPEN_OFFSET, split_header_offset, &split_header);
    records.extend_from_slice(&build_test_rar_qopen_record(
        QOPEN_OFFSET,
        whole_header_offset,
        &whole_header,
    ));
    records.extend_from_slice(&build_test_rar_qopen_record(
        QOPEN_OFFSET,
        QOPEN_OFFSET - 32,
        &build_test_rar_end_header(false),
    ));
    second.extend_from_slice(&build_test_rar_service_header("QO", records.len() as u64));
    second.extend_from_slice(&records);

    (
        vec![
            ("silver.horizon.part01.rar".to_string(), first),
            ("silver.horizon.part02.rar".to_string(), second),
        ],
        whole_header_offset,
    )
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
    // A set with tolerated members finalizes through a detached extraction
    // ticket; the shape is only final once that ticket has been taken.
    settle_direct_post_repair_work(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    (shape, working_dir)
}

/// [`run_direct_store_routing_only`], also reporting how many Quick Open
/// cross-check walks the job's first set ran while routing.
async fn run_direct_store_routing_only_counting_walks(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
) -> (String, u64) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = direct_store_job_spec("Silver Horizon", volumes);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, volumes, *file_index, *segment_number).await;
    }
    let walks = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set stays registered after routing")
        .router
        .quick_open_walks();
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    (shape, walks)
}

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
    demote_mid_download_for(
        temp_dir,
        job_id,
        volumes,
        DemotionReason::HoldsBudgetExceeded,
        before_demotion,
    )
    .await
}

/// [`demote_mid_download`] with the demotion reason spelled out, for the tests
/// that turn on the reason's [`VolumeDemand`] rather than on the sweep.
async fn demote_mid_download_for(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    reason: DemotionReason,
    before_demotion: impl FnOnce(&mut Pipeline, &std::path::Path),
) -> (Pipeline, std::path::PathBuf, u64) {
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download_leaving_the_sweep_outstanding(
            temp_dir,
            job_id,
            volumes,
            reason,
            before_demotion,
        )
        .await;
    // The demotion hands its reconstruction sweep to a detached worker and
    // returns; every assertion about materialized volumes, floors and requeued
    // articles is about what the ticket's completion does on the pipeline task.
    settle_direct_post_repair_work(&mut pipeline).await;
    (pipeline, working_dir, other_file_bytes)
}

/// [`demote_mid_download_for`] stopped where the demotion returns, with the
/// reconstruction sweep still an outstanding ticket. For the tests whose
/// subject is the detachment itself.
async fn demote_mid_download_leaving_the_sweep_outstanding(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    reason: DemotionReason,
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
    pipeline.demote_direct_set(job_id, 0, reason).await;
    (pipeline, working_dir, OTHER_FILE_BYTES)
}

/// The set the fixtures above demote: one member across three store volumes,
/// with volume 0 whole, volume 1 half covered and volume 2 not started.
fn demotion_fixture_volumes(member_name: &str) -> Vec<(String, Vec<u8>)> {
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    single_member_store_set(member_name, &payload, 3)
}

/// Every volume's payload article before any of its headers, so nothing has a
/// destination and the whole set piles up as holds.
fn payload_before_header_arrivals(volume_count: usize) -> Vec<(u32, u32)> {
    let mut arrivals: Vec<(u32, u32)> = (0..volume_count as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volume_count as u32).map(|index| (index, 0)));
    arrivals
}

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

/// No source volume of `volumes` that exists on disk holds a byte the set never
/// downloaded.
///
/// The assertion demotion has to satisfy however it goes: a reconstruction
/// writes the runs it verified, a refused run leaves its range untouched, a
/// refetch fallback leaves no file at all, and none of them may leave bytes that
/// were never downloaded looking like bytes that were.
///
/// A rebuilt volume is sparse where the sweep did not write — a run it could not
/// vouch for, an article that never arrived — so the check is per byte rather
/// than "a byte-exact prefix": every byte either is the posted volume's, or is a
/// zero standing for a hole the refetch fills. Only a non-zero byte that
/// disagrees is fabrication, and that is the failure this exists to catch.
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
        if let Some((offset, byte)) =
            written
                .iter()
                .zip(bytes.iter())
                .enumerate()
                .find_map(|(offset, (written, posted))| {
                    (written != posted && *written != 0).then_some((offset, *written))
                })
        {
            panic!(
                "{filename} was materialized with a byte the set never downloaded: \
                 {byte:#04x} at {offset}"
            );
        }
    }
}

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

/// Physical offset of the recovery record's data area inside a fixture volume.
///
/// Found by construction rather than by scanning for a byte pattern: the RR data
/// is the last `rr_bytes` before the end-of-archive header, whose encoded length
/// the builder fixes.
fn find_recovery_offset(volume: &[u8], rr_bytes: usize) -> usize {
    let end_header = build_test_rar_end_header(true).len();
    volume.len() - end_header - rr_bytes
}

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
    /// tolerance describes and the one the extraction can read back.
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
    /// nothing extracts it — so this is only good for what the
    /// *classification* decides.
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

/// The set's final router shape after every article of every volume.
async fn tolerance_shape(job_id: JobId, volumes: &[(String, Vec<u8>)]) -> String {
    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, _) = run_direct_store_routing_only(&temp_dir, job_id, volumes, &arrivals).await;
    shape
}

/// Where a store set's directory entries sit relative to its file members.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DirectoryPlacement {
    /// Written into the first volume, ahead of every member header — the shape
    /// an archiver produces when it walks a tree breadth-first.
    Leading,
    /// Written into the **last** volume, after the final part of the last
    /// member. This is the shape a folder-tree store set really has, and the
    /// one that used to spend a whole download only to demote on the closing
    /// volume's last article.
    Trailing,
}

/// A store set carrying `members` plus dataless directory entries.
///
/// `directories` is `(name, unix mode, mtime)`. The entries carry no data area,
/// so they cost the set nothing but a header.
fn store_set_with_directories(
    members: &[(&str, Vec<u8>)],
    volume_count: usize,
    directories: &[(&str, u32, u32)],
    placement: DirectoryPlacement,
) -> Vec<(String, Vec<u8>)> {
    assert!(volume_count >= 1);
    let total: usize = members.iter().map(|(_, bytes)| bytes.len()).sum();
    let chunk = total.div_ceil(volume_count);

    let mut spans = Vec::with_capacity(members.len());
    let mut cursor = 0usize;
    for (name, bytes) in members {
        spans.push((*name, bytes.as_slice(), cursor, cursor + bytes.len()));
        cursor += bytes.len();
    }

    let directory_headers: Vec<u8> = directories
        .iter()
        .flat_map(|(name, mode, mtime)| build_test_rar_directory_header(name, *mode, *mtime))
        .collect();

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
            if is_first && placement == DirectoryPlacement::Leading {
                bytes.extend_from_slice(&directory_headers);
            }

            for (name, payload, start, end) in &spans {
                let part_start = (*start).max(window_start);
                let part_end = (*end).min(window_end);
                if part_start >= part_end {
                    continue;
                }
                let part = &payload[part_start - start..part_end - start];
                let mut split_flags = 0u64;
                if part_start > *start {
                    split_flags |= 0x0008;
                }
                let split_after = part_end < *end;
                if split_after {
                    split_flags |= 0x0010;
                }
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

            if is_last && placement == DirectoryPlacement::Trailing {
                bytes.extend_from_slice(&directory_headers);
            }
            bytes.extend_from_slice(&build_test_rar_end_header(!is_last));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect()
}

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
    /// `(files stood in for, files read)` per direct verification pass, so a
    /// test can prove a pass read a volume rather than standing in for it on
    /// wire evidence.
    verify_read_splits: Vec<(usize, usize)>,
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
    run_repairable_par2_gate_inner(
        gate,
        job_id,
        member_name,
        volumes,
        par2_bytes,
        position,
        password,
        articles,
        None,
    )
    .await
}

/// [`run_repairable_par2_gate_with_articles`] with one volume that never
/// arrives.
///
/// `absent_ordinal` names a volume whose articles are all withheld — the
/// harness's stand-in for a volume every server answered `430` for. Nothing of
/// it is ever routed, so it has no envelope content, no covered run and no
/// staged image: the repair has to create its target from the length PAR2
/// describes and write every slice of it.
#[allow(clippy::too_many_arguments)]
async fn run_repairable_par2_gate_inner(
    gate: DirectStoreGate,
    job_id: JobId,
    member_name: &str,
    volumes: &[(String, Vec<u8>)],
    par2_bytes: &[u8],
    position: IndexPosition,
    password: Option<&str>,
    articles: usize,
    absent_ordinal: Option<u32>,
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
        if absent_ordinal == Some(ordinal) {
            continue;
        }
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
        verify_read_splits: pipeline.direct_verify_read_splits.clone(),
    }
}

/// Corrupts `len` bytes of one volume's **member payload**, leaving every
/// header and the end record intact.
///
/// The damage the wire cannot see. The harness delivers decoded bytes, so the
/// yEnc part CRC is computed over what is delivered and always agrees — exactly
/// as it does for an article a server corrupted before its own CRC was taken.
/// What disagrees is the archive's packed CRC32 for the part, one layer up.
fn damage_member_payload(volumes: &mut [(String, Vec<u8>)], volume: usize, len: usize) {
    let end_header = build_test_rar_end_header(volume + 1 < volumes.len());
    let bytes = &mut volumes[volume].1;
    let end = bytes.len() - end_header.len();
    let start = end - len;
    for byte in &mut bytes[start..end] {
        *byte ^= 0xA5;
    }
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

/// The KDF tuple every encrypted fixture here shares. `lg2 = 4` is 16 PBKDF2
/// rounds: real archives use 2^15 and up, and paying that per fixture would put
/// seconds into the suite for a number nothing under test reads.
const TEST_CRYPT_SALT: [u8; 16] = [0x5A; 16];

const TEST_CRYPT_IV: [u8; 16] = [0xA5; 16];

const TEST_CRYPT_KDF_LG2: u8 = 4;

/// A RAR4 end-of-archive header that **states its volume number**, which
/// [`build_test_rar4_end_header`] does not.
///
/// RAR4 has no per-volume number anywhere else — RAR5 carries one in the main
/// header, RAR4 carries it in `ENDARC` behind the `VOLUME_NUMBER` flag — and
/// `unrar-rs` reads `RarVolumeFacts::volume_number` from exactly there. Both
/// halves of a differential key volumes by the layout rather than by that
/// parsed number (an old-numbering set states none at all), but a numbered end
/// record is what modern RAR4 writers emit for `.partNN` sets, and it is the
/// cross-check the conventional path (`persist_rar_volume_facts`) holds the
/// layout against — so the realistic fixture states it.
fn build_test_rar4_end_header_numbered(more_volumes: bool, volume: u16) -> Vec<u8> {
    let mut flags: u16 = 0x0004; // VOLUME_NUMBER
    if more_volumes {
        flags |= 0x0001; // NEXT_VOLUME
    }
    build_test_rar4_block(0x7b, flags, &volume.to_le_bytes())
}
