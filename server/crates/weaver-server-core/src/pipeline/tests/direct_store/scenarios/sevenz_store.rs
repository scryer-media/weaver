//! Direct routing of 7z containers whose members are stored with the Copy
//! method.
//!
//! A 7z states its map at the *tail*, so these fixtures exercise the one thing
//! a RAR set never does: a layout that cannot be known until the last volume's
//! last article has landed, whatever order the articles arrive in.

use super::*;

use sevenz_turbo::encoder_options::AesEncoderOptions;
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};

/// One entry of a fixture archive.
struct Entry {
    name: &'static str,
    /// `None` for an entry the archive names but stores no bytes for. Such an
    /// entry has no stream at all — a zero-length stream is a different thing,
    /// and not one a 7z writer produces.
    bytes: Option<Vec<u8>>,
    /// The deletion marker an incremental archive carries.
    anti: bool,
    /// The entry's stored attribute word, when the fixture sets one.
    attributes: Option<u32>,
}

impl Entry {
    fn file(name: &'static str, bytes: Vec<u8>) -> Self {
        Self {
            name,
            bytes: Some(bytes),
            anti: false,
            attributes: None,
        }
    }

    fn empty_file(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: false,
            attributes: None,
        }
    }

    /// An entry the header marks for deletion rather than for writing.
    fn anti_item(name: &'static str) -> Self {
        Self {
            name,
            bytes: None,
            anti: true,
            attributes: None,
        }
    }

    /// An entry whose attributes say the name is a redirection, not a file.
    fn symlink(name: &'static str, target: &str) -> Self {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        Self {
            name,
            bytes: Some(target.as_bytes().to_vec()),
            anti: false,
            attributes: Some(FILE_ATTRIBUTE_REPARSE_POINT),
        }
    }
}

/// Deterministic member payload: compressible enough that a non-Copy fixture
/// really does shrink, and varied enough that a misrouted byte shows up.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let index = index as u64;
            (seed as u64)
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add(index.wrapping_mul(31))
                .rotate_left((index % 13) as u32) as u8
        })
        .collect()
}

/// Encodes a fixture archive in memory.
fn build_7z(entries: &[Entry], method: EncoderMethod, password: Option<&str>) -> Vec<u8> {
    build_7z_shaped(entries, method, password, false)
}

fn build_7z_shaped(
    entries: &[Entry],
    method: EncoderMethod,
    password: Option<&str>,
    encrypt_header: bool,
) -> Vec<u8> {
    let mut writer =
        ArchiveWriter::new(std::io::Cursor::new(Vec::new())).expect("create the 7z writer");
    let mut methods = vec![EncoderConfiguration::new(method)];
    if let Some(password) = password {
        // Encryption is applied to already-coded bytes, so it sits at the
        // output end of the chain.
        methods.insert(
            0,
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(password))),
        );
    }
    writer.set_content_methods(methods);
    if encrypt_header {
        writer.set_encrypt_header(true);
    }
    for entry in entries {
        let mut written = ArchiveEntry::new_file(entry.name);
        written.is_anti_item = entry.anti;
        if let Some(attributes) = entry.attributes {
            written.has_windows_attributes = true;
            written.windows_attributes = attributes;
        }
        match &entry.bytes {
            Some(bytes) => writer
                .push_archive_entry(written, Some(std::io::Cursor::new(bytes.clone())))
                .expect("write a file entry"),
            None => writer
                .push_archive_entry::<std::io::Cursor<Vec<u8>>>(written, None)
                .expect("write an entry with no stream"),
        };
    }

    writer.finish().expect("finish the archive").into_inner()
}

/// Cuts one container into `count` posted volumes, the way a 7z set is made:
/// a pure byte split at a fixed size, with no per-volume header. The last
/// volume is whatever is left, so an `archive` that does not divide evenly
/// produces the short tail real sets have.
fn split_volumes(archive: &[u8], count: usize) -> Vec<(String, Vec<u8>)> {
    assert!(count >= 1);
    if count == 1 {
        return vec![("silver.horizon.7z".to_string(), archive.to_vec())];
    }
    let chunk = archive.len().div_ceil(count);
    (0..count)
        .map(|index| {
            let start = index * chunk;
            let end = ((index + 1) * chunk).min(archive.len());
            (
                format!("silver.horizon.7z.{:03}", index + 1),
                archive[start..end].to_vec(),
            )
        })
        .collect()
}

/// [`direct_store_job_spec`] with the volumes' lengths stated **exactly**.
///
/// The shared helper pads each segment's `bytes=` with yEnc overhead, which is
/// what an NZB carries; a 7z set instead needs the decoded length its articles'
/// yEnc headers state, because the volumes are a byte split of one container
/// and their lengths are the only way a container offset becomes a (volume,
/// offset) pair. The test harness drives the yEnc layout from the assembly's
/// total, so the spec is where the true length has to be put.
fn sevenz_job_spec(volumes: &[(String, Vec<u8>)], articles: usize) -> JobSpec {
    JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        total_bytes: volumes.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
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
                            bytes: (end - start) as u32,
                            message_id: format!("sevenz-{index}-{segment_number}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// What one 7z gate run produced.
struct SevenZipOutcome {
    status: Option<JobStatus>,
    sets: String,
    /// Destination contents by name, for whatever the run was asked to read.
    files: BTreeMap<String, Option<Vec<u8>>>,
    /// Whether any destination name resolved to a directory.
    directories: BTreeMap<String, bool>,
    /// The most bytes the working directory held at any point in the run.
    peak_working_bytes: u64,
    /// What the destination holds once the run has finished.
    installed_bytes: u64,
}

/// Every payload byte resident under `root`. Sparse files are read through
/// their apparent length, which is the number this measurement wants: what the
/// subsystem asked the filesystem to hold, not what the filesystem chose to
/// allocate for it. Weaver's own dot-prefixed directory markers are not
/// payload and do not count.
fn bytes_on_disk(root: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
        .map(|entry| match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => bytes_on_disk(&entry.path()),
            Ok(_) => entry.metadata().map(|meta| meta.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

const ARTICLES_PER_VOLUME: usize = 2;

/// Services the pipeline's own queues until the job is terminal, taking each
/// completion tap as it is offered rather than waiting on any of them: a 7z set
/// can reach the end of a run with nothing owed on a given channel, and a
/// blocking read of one would then never return.
async fn drive_sevenz_to_terminal(
    pipeline: &mut Pipeline,
    job_id: JobId,
    working_dir: &std::path::Path,
    peak_working_bytes: &mut u64,
) {
    for _ in 0..256 {
        while let Ok(done) = pipeline.rar_refresh_done_rx.try_recv() {
            pipeline.handle_rar_refresh_done(done).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
        *peak_working_bytes = (*peak_working_bytes).max(bytes_on_disk(working_dir));
        pipeline.check_job_completion(job_id).await;
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            return;
        }
        if let Ok(done) = pipeline.move_done_rx.try_recv() {
            pipeline.handle_move_to_complete_done(done).await;
            continue;
        }
        if let Ok(done) = pipeline.extract_done_rx.try_recv() {
            pipeline.handle_extraction_done(done).await;
            continue;
        }
        tokio::task::yield_now().await;
        *peak_working_bytes = (*peak_working_bytes).max(bytes_on_disk(working_dir));
    }
}

async fn run_sevenz_gate(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    wanted: &[&str],
) -> SevenZipOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = sevenz_job_spec(volumes, ARTICLES_PER_VOLUME);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut peak_working_bytes = 0u64;
    for (file_index, segment_number) in arrivals {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            volumes,
            *file_index,
            *segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
        // Serviced between arrivals rather than only at the end: what the
        // directory holds while a set is mid-flight is the measurement this
        // subsystem exists to move, and it is unobservable once the whole set
        // has landed and finalized inside one drain.
        pump_pipeline_runtime_queues(&mut pipeline).await;
        peak_working_bytes = peak_working_bytes.max(bytes_on_disk(&working_dir));
    }
    drive_sevenz_to_terminal(&mut pipeline, job_id, &working_dir, &mut peak_working_bytes).await;
    let sets = format!(
        "status={:?} inflight_extractions={:?} inflight_moves={} sets={:?}",
        job_status_for_assert(&pipeline, job_id),
        pipeline.inflight_extractions.get(&job_id).map(|s| s.len()),
        pipeline.inflight_moves.len(),
        pipeline.direct_store.sets_for(job_id)
    );

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let mut files = BTreeMap::new();
    let mut directories = BTreeMap::new();
    for name in wanted {
        let candidates = [output_root.join(name), working_dir.join(name)];
        let found = candidates.iter().find(|path| path.exists());
        directories.insert((*name).to_string(), found.is_some_and(|path| path.is_dir()));
        files.insert(
            (*name).to_string(),
            found.and_then(|path| std::fs::read(path).ok()),
        );
    }
    SevenZipOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        sets,
        files,
        directories,
        peak_working_bytes,
        installed_bytes: bytes_on_disk(&output_root),
    }
}

/// Arrival plans over `volumes` volumes at two articles each.
fn tail_first(volume_count: usize) -> Vec<(u32, u32)> {
    let last = volume_count as u32 - 1;
    let mut plan = vec![(last, 1), (last, 0)];
    for file_index in 0..last {
        plan.push((file_index, 0));
        plan.push((file_index, 1));
    }
    plan
}

fn scrambled(volume_count: usize) -> Vec<(u32, u32)> {
    let mut plan = in_order_arrivals(volume_count);
    // A fixed, deliberately awkward permutation: every volume's second article
    // lands before its first, and the tail lands in the middle.
    plan.reverse();
    let by = 3 % plan.len();
    plan.rotate_left(by);
    plan
}

const MEMBER: &str = "Silver.Horizon.S01E01.dat";
const SECOND_MEMBER: &str = "Silver.Horizon.S01E02.dat";

#[tokio::test]
async fn sevenz_store_routes_a_split_set_in_arrival_order() {
    let member = payload(7, 40_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);

    for (label, arrivals) in [
        ("in order", in_order_arrivals(volumes.len())),
        ("tail first", tail_first(volumes.len())),
        ("scrambled", scrambled(volumes.len())),
    ] {
        let outcome = run_sevenz_gate(JobId(9_600), &volumes, &arrivals, &[MEMBER]).await;
        assert_eq!(
            outcome.files.get(MEMBER).cloned().flatten().as_deref(),
            Some(member.as_slice()),
            "{label}: the stored member must be routed byte for byte\nsets: {}",
            outcome.sets
        );
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{label}: the job must finish\nsets: {}",
            outcome.sets
        );
    }
}

#[tokio::test]
async fn sevenz_store_routes_a_single_volume_archive() {
    let member = payload(11, 25_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 1);
    let outcome = run_sevenz_gate(
        JobId(9_601),
        &volumes,
        &in_order_arrivals(volumes.len()),
        &[MEMBER],
    )
    .await;
    assert_eq!(
        outcome.files.get(MEMBER).cloned().flatten().as_deref(),
        Some(member.as_slice()),
        "sets: {}",
        outcome.sets
    );
}

#[tokio::test]
async fn sevenz_store_routes_two_members() {
    let first = payload(3, 30_000);
    let second = payload(5, 9_000);
    let archive = build_7z(
        &[
            Entry::file(MEMBER, first.clone()),
            Entry::file(SECOND_MEMBER, second.clone()),
        ],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let outcome = run_sevenz_gate(
        JobId(9_602),
        &volumes,
        &in_order_arrivals(volumes.len()),
        &[MEMBER, SECOND_MEMBER],
    )
    .await;
    assert_eq!(
        outcome.files.get(MEMBER).cloned().flatten().as_deref(),
        Some(first.as_slice()),
        "sets: {}",
        outcome.sets
    );
    assert_eq!(
        outcome
            .files
            .get(SECOND_MEMBER)
            .cloned()
            .flatten()
            .as_deref(),
        Some(second.as_slice()),
        "sets: {}",
        outcome.sets
    );
}

/// What the whole subsystem is for, stated as a number.
///
/// The conventional path writes the container and then writes the member again
/// as it extracts, so the working directory holds both at once. Routing writes
/// the member once and the container's non-member bytes — its headers — into a
/// sparse envelope, so the most the directory ever holds is one copy of the
/// payload plus the map that describes it.
#[tokio::test]
async fn sevenz_store_holds_one_copy_of_the_payload_and_no_container() {
    let member = payload(23, 120_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let conventional = archive.len() as u64 + member.len() as u64;

    let outcome = run_sevenz_gate(
        JobId(9_606),
        &volumes,
        &in_order_arrivals(volumes.len()),
        &[MEMBER],
    )
    .await;

    assert_eq!(
        outcome.files.get(MEMBER).cloned().flatten().as_deref(),
        Some(member.as_slice()),
        "sets: {}",
        outcome.sets
    );
    assert!(
        outcome.peak_working_bytes < archive.len() as u64,
        "the working directory must never hold the container: peak {} bytes \
         against a {}-byte container, and {conventional} for the conventional \
         path\nsets: {}",
        outcome.peak_working_bytes,
        archive.len(),
        outcome.sets
    );
    assert_eq!(
        outcome.installed_bytes,
        member.len() as u64,
        "the destination must hold the member and nothing else\nsets: {}",
        outcome.sets
    );
}

#[tokio::test]
async fn sevenz_store_creates_entries_the_archive_stores_no_bytes_for() {
    let member = payload(13, 20_000);
    let archive = build_7z(
        &[
            Entry::file(MEMBER, member.clone()),
            Entry::empty_file("Subs/Silver.Horizon.S01E01.idx"),
        ],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 2);
    let outcome = run_sevenz_gate(
        JobId(9_603),
        &volumes,
        &in_order_arrivals(volumes.len()),
        &[MEMBER, "Subs", "Subs/Silver.Horizon.S01E01.idx"],
    )
    .await;
    assert_eq!(
        outcome.files.get(MEMBER).cloned().flatten().as_deref(),
        Some(member.as_slice()),
        "sets: {}",
        outcome.sets
    );
    assert_eq!(
        outcome.directories.get("Subs"),
        Some(&true),
        "the entry's parent directory must exist\nsets: {}",
        outcome.sets
    );
    assert_eq!(
        outcome
            .files
            .get("Subs/Silver.Horizon.S01E01.idx")
            .cloned()
            .flatten(),
        Some(Vec::new()),
        "the archive's empty file must exist\nsets: {}",
        outcome.sets
    );
}

/// The eligibility matrix. Every shape here is one direct routing must decline
/// **as a whole set**: a 7z block is the unit of coding, so tolerating one
/// entry would mean decoding a container that has already been routed away.
/// Declining hands the archive to the conventional extractor with its volumes
/// still on disk, which is why each of these still finishes.
#[tokio::test]
async fn sevenz_store_declines_what_it_cannot_route() {
    let member = payload(17, 30_000);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "lzma2",
            build_7z(
                &[Entry::file(MEMBER, member.clone())],
                EncoderMethod::LZMA2,
                None,
            ),
        ),
        (
            "copy+aes",
            build_7z(
                &[Entry::file(MEMBER, member.clone())],
                EncoderMethod::COPY,
                Some("silver"),
            ),
        ),
        (
            "copy+aes header",
            build_7z_shaped(
                &[Entry::file(MEMBER, member.clone())],
                EncoderMethod::COPY,
                Some("silver"),
                true,
            ),
        ),
        (
            "an anti-item",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::anti_item("Silver.Horizon.S01E00.dat"),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "a redirection",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::symlink("Silver.Horizon.latest.dat", MEMBER),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
    ];

    for (label, archive) in cases {
        let volumes = split_volumes(&archive, 2);
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
        insert_active_job(&mut pipeline, JobId(9_604), spec).await;
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            submit_volume_article_of(
                &mut pipeline,
                JobId(9_604),
                &volumes,
                file_index,
                segment_number,
                ARTICLES_PER_VOLUME,
            )
            .await;
        }
        settle_direct_post_repair_work(&mut pipeline).await;
        let sets = format!("{:?}", pipeline.direct_store.sets_for(JobId(9_604)));
        assert!(
            sets.contains("Demoted"),
            "{label}: the set must be declined, not routed\nsets: {sets}"
        );
    }
}

/// A volume whose posted length disagrees with what the container's own
/// geometry implies. Nothing downstream can place a byte after that, so the
/// set is declined rather than routed against a map that does not close.
#[tokio::test]
async fn sevenz_store_declines_a_volume_whose_length_does_not_close_the_container() {
    let member = payload(19, 30_000);
    let mut archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    // Trailing bytes past the end header: the signature header's own
    // offset/size pair no longer sums to the container's length.
    archive.extend_from_slice(&[0u8; 64]);
    let volumes = split_volumes(&archive, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    insert_active_job(&mut pipeline, JobId(9_605), spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article_of(
            &mut pipeline,
            JobId(9_605),
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
    }
    settle_direct_post_repair_work(&mut pipeline).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(JobId(9_605)));
    assert!(
        sets.contains("Demoted"),
        "a container whose geometry does not close must be declined\nsets: {sets}"
    );
}

/// The same container with every stored CRC32 taken out of its end header.
///
/// 7z member checksums are optional and a writer that omits them produces a
/// perfectly ordinary archive, but `sevenz_turbo`'s writer always records one,
/// so the fixture has to be edited after the fact. All of a single-block
/// archive's digests live in one place — the SubStreamsInfo's `kCRC` property —
/// so removing it is a single, well-defined substitution, after which the end
/// header's own two checksums are recomputed. Every checksum written here is
/// **computed**, never a literal: a typed CRC is a fixture that agrees with
/// itself and with nothing else.
fn strip_member_crcs(archive: &[u8], member: &[u8]) -> Vec<u8> {
    const K_END: u8 = 0x00;
    const K_HEADER: u8 = 0x01;
    const K_SUB_STREAMS_INFO: u8 = 0x08;
    const K_CRC: u8 = 0x0A;

    let crc32 =
        |bytes: &[u8]| crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32;

    let mut start = [0u8; 32];
    start.copy_from_slice(&archive[..32]);
    let next_header_offset = u64::from_le_bytes(start[12..20].try_into().unwrap());
    let next_header_size = u64::from_le_bytes(start[20..28].try_into().unwrap());
    let header_start = (32 + next_header_offset) as usize;
    let header = &archive[header_start..header_start + next_header_size as usize];
    assert_eq!(
        header.first().copied(),
        Some(K_HEADER),
        "the fixture's end header must be the plain form for this edit to apply"
    );

    // `kSubStreamsInfo, kCRC, all-defined, <crc32>, kEnd` -> an empty
    // `kSubStreamsInfo`. The sizes and stream counts a reader needs are the
    // block's own, which is exactly what an omitted property means.
    let mut wanted = vec![K_SUB_STREAMS_INFO, K_CRC, 1];
    wanted.extend_from_slice(&crc32(member).to_le_bytes());
    wanted.push(K_END);
    let at = header
        .windows(wanted.len())
        .position(|window| window == wanted.as_slice())
        .expect("the member's recorded digest is in the end header");

    let mut rewritten = Vec::with_capacity(header.len());
    rewritten.extend_from_slice(&header[..at]);
    rewritten.extend_from_slice(&[K_SUB_STREAMS_INFO, K_END]);
    rewritten.extend_from_slice(&header[at + wanted.len()..]);

    let mut out = archive[..header_start].to_vec();
    out.extend_from_slice(&rewritten);
    out[20..28].copy_from_slice(&(rewritten.len() as u64).to_le_bytes());
    out[28..32].copy_from_slice(&crc32(&rewritten).to_le_bytes());
    let start_crc = crc32(&out[12..32]);
    out[8..12].copy_from_slice(&start_crc.to_le_bytes());
    out
}

/// A 7z set that is interrupted mid-download restarts correctly.
///
/// The map is cached, so the restart costs one article and not a container.
///
/// A 7z set's header bytes sit below the published floors, so they are never
/// refetched and the map cannot be re-read from the wire; it is cached on the
/// same rows the volume lengths are, and restore re-resolves the one against
/// the other. Three things had to hold for that to be reachable: conventional
/// discovery must not delete rows for a set whose volumes are deliberately not
/// files, the checkpoint's volume claims must be validated against the lengths
/// rather than against RAR volume headers, and a restored container volume is
/// confirmed by its map rather than by a per-volume walk it never had.
#[tokio::test]
async fn a_sevenz_set_restarts_without_materializing_a_volume() {
    let member = payload(29, 60_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let job_id = JobId(9_607);
    let temp_dir = tempfile::tempdir().unwrap();

    // Everything but one mid-container article, so the tail — and with it the
    // end header — is already parsed when the process goes down.
    let withheld = (1, 1);
    let arrivals: Vec<(u32, u32)> = in_order_arrivals(volumes.len())
        .into_iter()
        .filter(|arrival| *arrival != withheld)
        .collect();

    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in &arrivals {
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                *file_index,
                *segment_number,
                ARTICLES_PER_VOLUME,
            )
            .await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME),
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
        queued,
        vec![withheld],
        "a restored 7z set must re-derive its layout from the cached container map and ask \
         only for the article that never arrived"
    );
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted()),
        "an interrupted 7z set must come back routing, not demoted"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
    }
    let mut peak_working_bytes = 0u64;
    drive_sevenz_to_terminal(&mut pipeline, job_id, &working_dir, &mut peak_working_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted 7z set must still never materialize a source volume\nsets: {sets}"
    );
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let restored = [output_root.join(MEMBER), working_dir.join(MEMBER)]
        .iter()
        .find_map(|path| std::fs::read(path).ok());
    assert_eq!(
        restored.as_deref(),
        Some(member.as_slice()),
        "the member must survive the restart byte for byte\nsets: {sets}"
    );
}

/// A member the archive records no checksum for publishes on the recovery
/// set's verdict — and on nothing before it.
///
/// The ordering is the whole point. Full coverage marks such a member verified,
/// because coverage of the declared size is the only evidence direct routing
/// has, but `finalize_ready_direct_sets` refuses to commit any set of a
/// par2-bearing job until that job is verified or bypassed. So an article that
/// never arrives is repaired *first* and published *after*, rather than
/// published on coverage and corrected afterwards.
#[tokio::test]
async fn sevenz_store_member_without_crc_finalizes_on_par2() {
    let member = payload(23, 24_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let archive = strip_member_crcs(&archive, &member);
    let volumes = split_volumes(&archive, 2);
    let par2_bytes = repairable_par2_index(&volumes, 64);
    let job_id = JobId(9_606);
    let lost = (0u32, 1u32);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let mut spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    let index_file_index = append_par2_index(&mut spec, &par2_bytes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == lost {
            continue;
        }
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        pipeline.direct_store.finalized_sets, 0,
        "no set may commit before the recovery set has spoken"
    );
    assert!(
        !output_root.join(MEMBER).exists(),
        "the member must not be published before the recovery set has spoken"
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

    // The lost article is never coming and the harness has no server to say so;
    // draining the queues is what makes the download pipeline look exhausted,
    // which is the condition every PAR2 gate waits for before treating a hole
    // as damage rather than as work still in flight.
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
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
        if let Some(done) = next_owed_extraction(&mut pipeline, job_id).await {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
    }

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let published = std::fs::read(output_root.join(MEMBER))
        .ok()
        .or_else(|| staging_member(&complete_dir, MEMBER));
    assert_eq!(
        published.as_deref(),
        Some(member.as_slice()),
        "the repaired member must be published whole\nsets: {sets}"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "sets: {sets}"
    );
}

/// The fixtures themselves, read back through the library at full size.
///
/// A routing refusal and a fixture the library cannot read look identical from
/// the outside, so this pins the difference: every archive these tests build is
/// a valid 7z that parses from a complete image. What weaver then decides about
/// it is the subject of the scenarios above.
#[test]
fn every_fixture_is_a_readable_archive() {
    let member = payload(29, 12_000);
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "one member",
            build_7z(
                &[Entry::file(MEMBER, member.clone())],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "two members",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::file(SECOND_MEMBER, payload(31, 3_000)),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "an entry with no bytes",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::empty_file("Subs/Silver.Horizon.S01E01.idx"),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "an anti-item",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::anti_item("Silver.Horizon.S01E00.dat"),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "a redirection",
            build_7z(
                &[
                    Entry::file(MEMBER, member.clone()),
                    Entry::symlink("Silver.Horizon.latest.dat", MEMBER),
                ],
                EncoderMethod::COPY,
                None,
            ),
        ),
        (
            "no member checksum",
            strip_member_crcs(
                &build_7z(
                    &[Entry::file(MEMBER, member.clone())],
                    EncoderMethod::COPY,
                    None,
                ),
                &member,
            ),
        ),
    ];
    for (label, archive) in cases {
        let read = sevenz_turbo::Archive::read(
            &mut std::io::Cursor::new(archive.clone()),
            &sevenz_turbo::Password::empty(),
        );
        assert!(read.is_ok(), "{label}: {:?}", read.err());
    }
}

/// [`sevenz_job_spec`] for a set the job carries a password for.
fn sevenz_job_spec_with_password(
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    password: &str,
) -> JobSpec {
    let mut spec = sevenz_job_spec(volumes, articles);
    spec.password = Some(password.to_string());
    spec
}

/// The same container with one byte of its end header flipped.
///
/// The signature header still places the end header exactly at the container's
/// last byte, so the geometry closes and the bytes are all there; what no
/// longer holds is the end header's own content. Nothing about that changes
/// when the rest of the container arrives.
fn corrupt_end_header(archive: &[u8]) -> Vec<u8> {
    let mut out = archive.to_vec();
    let offset = u64::from_le_bytes(out[12..20].try_into().expect("eight bytes")) as usize;
    let at = SIGNATURE_HEADER_LEN + offset + 1;
    out[at] ^= 0xFF;
    out
}

const SIGNATURE_HEADER_LEN: usize = 32;

/// What makes a split container routable at all rather than merely readable.
///
/// A container states its map at the tail, and its volume boundaries one
/// volume at a time — each volume declares its own length on any one of its
/// articles, and the map cannot be resolved until every one of them has. A set
/// that simply waited for those lengths to turn up in dispatch order would
/// hold every volume ahead of the last as holds: the whole container staged
/// before a byte routes, which is the opposite of what direct routing is for.
/// So admission reaches past its own retention limit for exactly the articles
/// the parse needs — the first of every volume that has not declared a length,
/// and the last of the last volume, which carries the end header.
///
/// The arrangement below is what makes that visible. The first volume's
/// articles arrive before the limit tightens and stage as holds, because there
/// is no map to route them against; from there nothing fits, and the only
/// articles the set can be handed are the ones it reaches past the limit for.
/// A probe that named one volume at a time would stop at the last volume and
/// leave the middle ones' lengths unstated forever.
#[tokio::test]
async fn sevenz_store_probes_one_article_per_volume_rather_than_staging_the_container() {
    const VOLUMES: usize = 4;
    const ARTICLES: usize = 20;
    /// Articles of the first volume that land before the limit tightens.
    const HELD: u32 = 16;
    let job_id = JobId(9_608);
    let member = payload(37, 32_600);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, VOLUMES);
    let article = (volumes[0].1.len() as u64).div_ceil(ARTICLES as u64);
    // Capacity for what is already held plus the parse's own articles, and for
    // nothing else. The holds budget is a fraction of one volume, so the
    // volumes in the middle could not be held in RAM even if the set tried.
    let capacity = (HELD as u64 + VOLUMES as u64 + 2) * article;
    let holds_budget = 2 * article;
    assert!(
        (VOLUMES as u64 - 1) * article * ARTICLES as u64 > capacity,
        "the volumes ahead of the tail must not fit, or the probe proves nothing"
    );

    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline_with_buffers(
        &temp,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        4,
    )
    .await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(holds_budget);
    pipeline
        .direct_store
        .set_holds_scratch_ceiling(capacity - holds_budget);
    let spec = sevenz_job_spec(&volumes, ARTICLES);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The first volume, most of the way in. Nothing routes: a container states
    // its map once, at the tail, so every one of these is a hold.
    for segment_number in 0..HELD {
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
        submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, segment_number, ARTICLES)
            .await;
    }
    let held = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set is admitted");
    assert!(
        held.router.staged_bytes() > 0
            && held.router.staged_bytes() + article > held.router.holds_admission_limit(),
        "the limit must have no room left, or nothing below is a probe"
    );

    // Articles the set was handed although its retention limit had no room for
    // them: the probe wave, counted rather than inferred.
    let mut over_limit = 0usize;
    let mut probed_volumes: Vec<(u32, u32)> = Vec::new();
    let mut peak_staged = 0u64;
    for _ in 0..512 {
        let Some(lease) = pipeline.lease_for_server_for_test(0) else {
            break;
        };
        if lease.works.is_empty() {
            break;
        }
        for work in lease.works {
            if let Some(set) = pipeline.direct_store.set(job_id, 0) {
                let available = set
                    .router
                    .holds_admission_limit()
                    .saturating_sub(set.router.staged_bytes());
                if work.byte_estimate as u64 > available {
                    over_limit += 1;
                    probed_volumes.push((
                        work.segment_id.file_id.file_index,
                        work.segment_id.segment_number,
                    ));
                }
            }
            let id = work.segment_id;
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                id.file_id.file_index,
                id.segment_number,
                ARTICLES,
            )
            .await;
            if let Some(set) = pipeline.direct_store.set(job_id, 0) {
                peak_staged = peak_staged.max(set.router.staged_bytes());
                assert!(
                    !set.is_demoted(),
                    "the set must route rather than stage its way into a demotion\nsets: {:?}",
                    pipeline.direct_store.sets_for(job_id)
                );
            }
        }
    }

    let mut peak_working_bytes = 0u64;
    drive_sevenz_to_terminal(&mut pipeline, job_id, &working_dir, &mut peak_working_bytes).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        peak_staged < capacity,
        "the set must never have held the container it could not hold\nsets: {sets}"
    );
    // The first volume has already stated its length, so what is left is one
    // article for each of the other three — whichever of that volume's queued
    // articles comes up first, since any of them states its length — and the
    // last volume's last article, which is where the end header is.
    let mut probed: Vec<u32> = probed_volumes.iter().map(|(volume, _)| *volume).collect();
    probed.sort_unstable();
    assert_eq!(
        probed,
        vec![1, 2, 3, 3],
        "the parse must cost one article per undeclared volume plus the tail\nprobed: \
         {probed_volumes:?}\nsets: {sets}"
    );
    assert!(
        probed_volumes.contains(&(VOLUMES as u32 - 1, ARTICLES as u32 - 1)),
        "one of them must be the article the end header is in\nprobed: {probed_volumes:?}"
    );
    assert_eq!(over_limit, probed_volumes.len());
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let routed = [output_root.join(MEMBER), working_dir.join(MEMBER)]
        .iter()
        .find_map(|path| std::fs::read(path).ok());
    assert_eq!(
        routed.as_deref(),
        Some(member.as_slice()),
        "the member must land byte for byte\nsets: {sets}"
    );
}

/// An end header that is entirely present and still does not parse is a
/// verdict, not a shortage.
///
/// The distinction is what keeps a malformed container from costing a set its
/// whole holds budget: the reader cannot tell a hole in the sparse image from
/// the end of a file, so without it every parse error would read as "wait for
/// more" until there was nothing left to wait for.
#[tokio::test]
async fn sevenz_store_declines_an_end_header_it_cannot_parse() {
    let member = payload(43, 30_000);
    let archive = corrupt_end_header(&build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    ));
    let volumes = split_volumes(&archive, 2);
    let job_id = JobId(9_609);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // The signature header, and the tail that carries the end header. Between
    // them both volumes have declared their lengths, so the container's map is
    // as readable as it is ever going to be — while half of it is still
    // outstanding.
    for (file_index, segment_number) in [(0, 0), (1, 1)] {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
    }
    settle_direct_post_repair_work(&mut pipeline).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted"),
        "an unreadable end header must be refused while the rest is outstanding\nsets: {sets}"
    );
}

/// A header-encrypted container opens with the job's password.
///
/// `-mhe` puts the map itself in an encrypted block, so with no key there is
/// nothing to route and nothing to say about the archive beyond that. With the
/// key the map reads, and the verdict moves to what the map actually says:
/// `7z a -mhe=on -p` encrypts the content too, and an encrypted stored member
/// is reported rather than routed. Reaching that refusal is the proof the
/// header opened.
#[tokio::test]
async fn sevenz_store_opens_a_header_encrypted_container_with_the_job_password() {
    let member = payload(47, 30_000);
    let archive = build_7z_shaped(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        Some("silver"),
        true,
    );
    let volumes = split_volumes(&archive, 2);
    let job_id = JobId(9_610);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec_with_password(&volumes, ARTICLES_PER_VOLUME, "silver");
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
    }
    settle_direct_post_repair_work(&mut pipeline).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("EncryptedContent"),
        "the keyed header must be read, and its encrypted content reported\nsets: {sets}"
    );
    assert!(
        !sets.contains("EncryptedHeader"),
        "a container the job holds the password for must not refuse unread\nsets: {sets}"
    );
}

/// The completion gate's installed-members clause is 7z-only, in both of a RAR
/// direct set's states.
///
/// A job whose archives are all RAR never reaches the readiness check the
/// clause lives in — the completion gate sends it to the RAR check instead —
/// so a RAR direct set has never needed it. Pinned here because the clause
/// sits on a function a mixed job's RAR volumes do reach.
#[tokio::test]
async fn a_rar_direct_set_is_never_counted_as_installed() {
    let member = payload(53, 8_400);
    let volumes = single_member_store_set("Silver.Horizon.S01E03.dat", &member, 4);
    let job_id = JobId(9_611);
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Silver Horizon", &volumes),
    )
    .await;

    fn any_installed(pipeline: &Pipeline, job_id: JobId) -> bool {
        pipeline
            .jobs
            .get(&job_id)
            .expect("the job is active")
            .assembly
            .files()
            .any(|file| pipeline.direct_set_already_installed(job_id, file))
    }

    assert!(
        !any_installed(&pipeline, job_id),
        "a RAR direct set mid-download owns no installed member"
    );
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    settle_direct_post_repair_work(&mut pipeline).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.is_finalized()),
        "the RAR set must finalize for the second half of this to mean anything\nsets: {sets}"
    );
    assert!(
        !any_installed(&pipeline, job_id),
        "a finalized RAR direct set must stay invisible to the 7z clause\nsets: {sets}"
    );
}
