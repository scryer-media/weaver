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
    sevenz_job_spec_stating(volumes, articles, |decoded| decoded)
}

/// [`sevenz_job_spec`] with each segment's `bytes=` passed through `stated`,
/// for a test that needs the NZB to say something other than the exact decoded
/// length — an encoded size, the way a real NZB states it.
fn sevenz_job_spec_stating(
    volumes: &[(String, Vec<u8>)],
    articles: usize,
    stated: impl Fn(u32) -> u32,
) -> JobSpec {
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
                            bytes: stated((end - start) as u32),
                            message_id: format!("sevenz-{index}-{segment_number}@example.com"),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// [`submit_volume_article_of`] with the article's yEnc header stating a file
/// size of the caller's choosing.
///
/// `0` is what the decoder reports for an article whose `=ybegin` carries no
/// `size=` at all, so it is how a volume that states nothing is posted here.
async fn submit_volume_article_declaring(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
    declared_file_len: u64,
) {
    let (filename, bytes) = &volumes[file_index as usize];
    let (start, end) = article_extent(bytes.len(), segment_number, ARTICLES_PER_VOLUME);
    let data = &bytes[start..end];
    let file_id = NzbFileId { job_id, file_index };
    let total_segments = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("active test file assembly")
        .total_segments();
    let checkpoint_plan = pipeline.par2_checkpoint_plan(job_id);
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
                    file_size: declared_file_len,
                    part: Some(segment_number + 1),
                    total: Some(total_segments),
                    begin: Some(start as u64 + 1),
                    end: Some((start + data.len()) as u64),
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(data),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan,
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

/// Every distinct shape the job's direct sets passed through, in the order it
/// was seen.
///
/// A verdict is not a state a set rests in. Demoting one hands its volumes
/// back and retires the set, so a run that states a refusal usually has
/// nothing left to read it off by the time it ends. Asking the live list
/// afterwards therefore asks whether the read happened to fall inside that
/// window — a question about how busy the machine was, not about what the
/// router decided. This is the record the window cannot close on.
#[derive(Default)]
struct SetWitness {
    seen: Vec<String>,
}

impl SetWitness {
    /// Takes the sets' shape as it stands, keeping it when it differs from the
    /// last one taken.
    fn observe(&mut self, pipeline: &Pipeline, job_id: JobId) {
        let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
        if self.seen.last() != Some(&shape) {
            self.seen.push(shape);
        }
    }

    /// Whether any shape the run passed through carried `marker`.
    fn contains(&self, marker: &str) -> bool {
        self.seen.iter().any(|shape| shape.contains(marker))
    }

    fn render(&self) -> String {
        self.seen.join(" then ")
    }
}

/// Drains the asynchronous work a verdict starts of its own accord, recording
/// the sets on either side of it.
///
/// Both of these wait on a channel rather than on a number of turns, so what
/// they leave behind is the same whatever else the machine is running.
async fn settle_sevenz_verdict(pipeline: &mut Pipeline, job_id: JobId, witness: &mut SetWitness) {
    witness.observe(pipeline, job_id);
    settle_direct_post_repair_work(pipeline).await;
    witness.observe(pipeline, job_id);
    settle_direct_demotion_work(pipeline).await;
    witness.observe(pipeline, job_id);
}

/// Everything a turn would have to change for the run to have moved at all.
///
/// Nothing in it is a clock. A turn that leaves all of it identical moved
/// nothing, and because the turn that produced it first drained every channel
/// and waited out everything in flight, there is nothing left for a further
/// turn to pick up.
fn sevenz_run_fingerprint(pipeline: &Pipeline, job_id: JobId, working_bytes: u64) -> String {
    format!(
        "{:?}|{:?}|{}|{:?}|{}|{}|{working_bytes}",
        job_status_for_assert(pipeline, job_id),
        pipeline.direct_store.sets_for(job_id),
        pipeline.inflight_moves.len(),
        pipeline
            .inflight_extractions
            .get(&job_id)
            .map(|sets| sets.len()),
        pipeline.direct_demotion_in_flight.len(),
        pipeline.pending_completion_checks.len(),
    )
}

/// Services the pipeline's own queues until the run has produced what the
/// caller came for.
///
/// Three things end it and none of them is a number of turns. A job that
/// reaches a terminal status is finished. A run whose whole subject is a
/// verdict ends as soon as `awaited` appears in the witness. And a turn that
/// drained every channel, waited out everything in flight and still changed
/// nothing has proved there is no further progress to be had. Counting turns
/// instead would make the outcome a function of how loaded the machine is:
/// a demotion sweeps and hands back off-thread, and a fixed count can stop on
/// either side of that.
async fn drive_sevenz(
    pipeline: &mut Pipeline,
    job_id: JobId,
    working_dir: &std::path::Path,
    peak_working_bytes: &mut u64,
    awaited: Option<&str>,
    witness: &mut SetWitness,
) {
    loop {
        let before = sevenz_run_fingerprint(pipeline, job_id, *peak_working_bytes);
        while let Ok(done) = pipeline.rar_refresh_done_rx.try_recv() {
            pipeline.handle_rar_refresh_done(done).await;
        }
        pump_pipeline_runtime_queues(pipeline).await;
        *peak_working_bytes = (*peak_working_bytes).max(bytes_on_disk(working_dir));
        pipeline.check_job_completion(job_id).await;
        settle_sevenz_verdict(pipeline, job_id, witness).await;
        if matches!(
            job_status_for_assert(pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            return;
        }
        if awaited.is_some_and(|marker| witness.contains(marker)) {
            return;
        }
        // Waited for when the job owes one, taken when it is already there, and
        // otherwise a turn yielded to whatever is running behind this one.
        if let Some(done) = next_owed_extraction(pipeline, job_id).await {
            pipeline.handle_extraction_done(done).await;
            continue;
        }
        if let Ok(done) = pipeline.move_done_rx.try_recv() {
            pipeline.handle_move_to_complete_done(done).await;
            continue;
        }
        *peak_working_bytes = (*peak_working_bytes).max(bytes_on_disk(working_dir));
        witness.observe(pipeline, job_id);
        if sevenz_run_fingerprint(pipeline, job_id, *peak_working_bytes) == before {
            return;
        }
    }
}

async fn run_sevenz_gate(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[(u32, u32)],
    wanted: &[&str],
) -> SevenZipOutcome {
    run_sevenz_gate_declaring(job_id, volumes, &BTreeMap::new(), arrivals, wanted).await
}

/// [`run_sevenz_gate`] with the length some volumes' articles *declare* chosen
/// by the caller, by volume index.
///
/// The NZB stays honest and every article carries its real bytes; what changes
/// is the one number a container's geometry is hinted by. `=ybegin size=` is a
/// declaration, not a measurement, and this is the only way to post one that
/// is wrong without also making the posting inconsistent with itself.
async fn run_sevenz_gate_declaring(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    declared: &BTreeMap<u32, u64>,
    arrivals: &[(u32, u32)],
    wanted: &[&str],
) -> SevenZipOutcome {
    run_sevenz_gate_awaiting(job_id, volumes, declared, arrivals, wanted, None).await
}

/// [`run_sevenz_gate_declaring`] for a run whose subject is a verdict rather
/// than a finished job.
///
/// `awaited` is the substring of a set's shape the caller is going to assert
/// on. A demotion leaves the job needing volumes this harness has no server to
/// refetch from, so such a run never reaches a terminal status and there is
/// nothing else for it to stop on; naming the verdict is what makes the stop
/// an observation rather than a guess.
async fn run_sevenz_gate_awaiting(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    declared: &BTreeMap<u32, u64>,
    arrivals: &[(u32, u32)],
    wanted: &[&str],
    awaited: Option<&str>,
) -> SevenZipOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = sevenz_job_spec(volumes, ARTICLES_PER_VOLUME);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut peak_working_bytes = 0u64;
    // Observed from the first article on, not only once the arrivals are done:
    // a container can be refused on the very article that states its geometry,
    // and the set it is refused on is gone by the end of the loop.
    let mut witness = SetWitness::default();
    for (file_index, segment_number) in arrivals {
        match declared.get(file_index) {
            Some(stated) => {
                submit_volume_article_declaring(
                    &mut pipeline,
                    job_id,
                    volumes,
                    *file_index,
                    *segment_number,
                    *stated,
                )
                .await;
            }
            None => {
                submit_volume_article_of(
                    &mut pipeline,
                    job_id,
                    volumes,
                    *file_index,
                    *segment_number,
                    ARTICLES_PER_VOLUME,
                )
                .await;
            }
        }
        // Serviced between arrivals rather than only at the end: what the
        // directory holds while a set is mid-flight is the measurement this
        // subsystem exists to move, and it is unobservable once the whole set
        // has landed and finalized inside one drain.
        witness.observe(&pipeline, job_id);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;
        peak_working_bytes = peak_working_bytes.max(bytes_on_disk(&working_dir));
    }
    drive_sevenz(
        &mut pipeline,
        job_id,
        &working_dir,
        &mut peak_working_bytes,
        awaited,
        &mut witness,
    )
    .await;
    let sets = format!(
        "status={:?} inflight_extractions={:?} inflight_moves={} sets={}",
        job_status_for_assert(&pipeline, job_id),
        pipeline.inflight_extractions.get(&job_id).map(|s| s.len()),
        pipeline.inflight_moves.len(),
        witness.render()
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

/// A volume whose posted length disagrees with the place the geometry gives
/// it. The part size and the total are volume zero's to state; every other
/// volume's declaration is checked against them, and the first one that
/// disagrees ends the route before a byte is written against a map that does
/// not close. Here the container carries 64 bytes past its own end header, so
/// the last volume is that much longer than its place allows.
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
    restart_a_sevenz_set(
        JobId(9_607),
        ARTICLES_PER_VOLUME,
        (1, 1),
        |decoded| decoded,
        &[(1, 1)],
    )
    .await;
}

/// The same restart with the NZB stating its segments the way a real one does:
/// the yEnc-**encoded** size, a few percent over the payload.
///
/// Restore commits the skipped segments into the assembly at that stated size,
/// so the restored volumes read a few percent longer than the part size the
/// geometry requires. The length check is the authoritative one for a
/// container volume, and taken at that number it would demote a set whose
/// bytes are exactly where the map placed them — a whole-set materialization
/// and redownload after every restart, for arithmetic. A restored volume's
/// honest length is its coverage end, which is what the check must be given.
///
/// Four articles a volume, the last one withheld: the decoded floor vouches
/// for the first two at their **encoded** size, the restore asks for the other
/// two (the third is durable, but the encoded walk cannot prove it — the
/// documented cost of the floor convention, shared with every format), and the
/// volume completes at an encoded prefix plus a decoded remainder, a few
/// hundred bytes over what the map says it is.
#[tokio::test]
async fn a_restarted_sevenz_set_is_not_demoted_for_its_encoded_segment_sizes() {
    let witness = restart_a_sevenz_set(
        JobId(9_621),
        4,
        (1, 3),
        |decoded| decoded * 103 / 100 + 64,
        &[(1, 2), (1, 3)],
    )
    .await;
    assert!(
        !witness.contains("Demoted"),
        "a restored volume's stated size must never be held against the geometry\nsets: {}",
        witness.render()
    );
}

/// The wholly missing volume again, protected by PAR3 rather than PAR2. The
/// 7z set's geometry and map are the same; what differs is the repair seam —
/// PAR3's readback routes the rebuilt volume slice by slice and confirms it
/// through `note_volume_complete` itself — so both the middle and the tail
/// absence are driven through it.
async fn a_wholly_missing_sevenz_volume_under_par3(
    job_id: JobId,
    missing: u32,
) -> Par3RepairOutcome {
    let member = payload(37, 30_000);
    let second = payload(41, 6_000);
    let archive = build_7z(
        &[
            Entry::file(MEMBER, member.clone()),
            Entry::file(SECOND_MEMBER, second.clone()),
        ],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let carriers = par3_carriers_over(&volumes, PAR2_SLICE_BYTES, 96);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    let outcome = run_direct_set_with_par3(
        job_id,
        spec,
        &volumes,
        ARTICLES_PER_VOLUME,
        Some(missing),
        &carriers,
    )
    .await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        outcome.status,
        outcome.shapes()
    );
    for (name, expected) in [(MEMBER, &member), (SECOND_MEMBER, &second)] {
        assert_eq!(
            outcome.member(name).as_ref(),
            Some(expected),
            "{name} must be published whole\nsets: {}",
            outcome.shapes()
        );
    }
    let published: Vec<String> = std::fs::read_dir(&outcome.output_root)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| {
                    let len = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
                    format!("{} ({len} B)", entry.file_name().to_string_lossy())
                })
                .collect()
        })
        .unwrap_or_default();
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| outcome.output_root.join(filename).exists()),
        "no volume may be published by name; published = {published:?}, finalized = {}, \
         materialized = {}, scratch left = {}\nsets: {}",
        outcome.finalized,
        outcome.materialized,
        outcome.repair_scratch_left,
        outcome.shapes()
    );
    outcome
}

#[tokio::test]
async fn sevenz_store_par3_rebuilds_a_wholly_missing_middle_volume() {
    let outcome = a_wholly_missing_sevenz_volume_under_par3(JobId(9_623), 1).await;
    assert!(
        !outcome.demoted(),
        "a missing middle volume is rebuilt into the live set\nsets: {}",
        outcome.shapes()
    );
    assert_eq!(
        outcome.finalized,
        1,
        "the set must commit its own partials\nsets: {}",
        outcome.shapes()
    );
    assert!(
        !outcome.volume_file_seen,
        "no source volume may appear under its own name\nsets: {}",
        outcome.shapes()
    );
}

#[tokio::test]
async fn sevenz_store_par3_rebuilds_a_wholly_missing_tail_volume() {
    // The tail holds the end header, so the set never learns its map and is
    // demoted for an unreadable one; the repair then lands on disk and the
    // conventional extractor publishes the members.
    let outcome = a_wholly_missing_sevenz_volume_under_par3(JobId(9_624), 2).await;
    assert!(
        outcome.demoted(),
        "a 7z set whose end header never arrives cannot stay direct\nsets: {}",
        outcome.shapes()
    );
}

/// Downloads all but one mid-container article, shuts down, restores, and
/// drives the restored set to its end; returns the shapes it passed through.
///
/// `expected_queue` is what the restore may ask for again: the withheld
/// article, plus whatever durable neighbours the encoded-size walk cannot
/// prove covered.
async fn restart_a_sevenz_set(
    job_id: JobId,
    articles: usize,
    withheld: (u32, u32),
    stated: impl Fn(u32) -> u32 + Copy,
    expected_queue: &[(u32, u32)],
) -> SetWitness {
    let member = payload(29, 60_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let temp_dir = tempfile::tempdir().unwrap();

    // Everything but one mid-container article, so the tail — and with it the
    // end header — is already parsed when the process goes down.
    let arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32)
        .flat_map(|file_index| (0..articles as u32).map(move |article| (file_index, article)))
        .filter(|arrival| *arrival != withheld)
        .collect();

    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let spec = sevenz_job_spec_stating(&volumes, articles, stated);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in &arrivals {
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                *file_index,
                *segment_number,
                articles,
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
            spec: sevenz_job_spec_stating(&volumes, articles, stated),
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
        queued, expected_queue,
        "a restored 7z set must re-derive its layout from the cached container map and ask \
         only for the article that never arrived (and any the floor walk cannot vouch for)"
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
            articles,
        )
        .await;
    }
    let mut peak_working_bytes = 0u64;
    let mut witness = SetWitness::default();
    drive_sevenz(
        &mut pipeline,
        job_id,
        &working_dir,
        &mut peak_working_bytes,
        None,
        &mut witness,
    )
    .await;

    let sets = witness.render();
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
    witness
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

/// What a run with one whole volume withheld and a recovery set on hand left
/// behind.
struct MissingVolumeRun {
    pipeline: Pipeline,
    _temp_dir: tempfile::TempDir,
    output_root: std::path::PathBuf,
    witness: SetWitness,
    volumes: Vec<(String, Vec<u8>)>,
    members: Vec<(&'static str, Vec<u8>)>,
    job_id: JobId,
}

/// Downloads every article of a three-volume set except those of
/// `missing_volume`, hands the job its recovery set, and drives it to a
/// terminal status.
async fn run_with_a_wholly_missing_volume(job_id: JobId, missing_volume: u32) -> MissingVolumeRun {
    let member = payload(37, 30_000);
    let second = payload(41, 6_000);
    let archive = build_7z(
        &[
            Entry::file(MEMBER, member.clone()),
            Entry::file(SECOND_MEMBER, second.clone()),
        ],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let par2_bytes = repairable_par2_index(&volumes, 96);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let mut spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    let index_file_index = append_par2_index(&mut spec, &par2_bytes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let mut witness = SetWitness::default();
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index == missing_volume {
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
        witness.observe(&pipeline, job_id);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        pipeline.direct_store.finalized_sets, 0,
        "no set may commit before the recovery set has spoken"
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

    // The harness delivered the other volumes' articles without leasing them,
    // and the missing volume's are never coming with no server to say so. Both
    // are what a leased article looks like from here: gone from the queue and
    // either answered or not. Re-leased every turn, because a probe that wants
    // the tail re-asks for it and a refetch re-asks for everything, and
    // "nothing more is coming" is what every PAR2 gate waits for before reading
    // a hole as damage. A re-asked article of a volume the wire *does* hold is
    // answered again, the way a refetch is.
    let lease_the_queue = |pipeline: &mut Pipeline| -> Vec<(u32, u32)> {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue = crate::DownloadQueue::new();
        state
            .download_queue
            .drain_all()
            .into_iter()
            .filter(|work| work.segment_id.file_id.file_index < volumes.len() as u32)
            .map(|work| {
                (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                )
            })
            .collect()
    };
    lease_the_queue(&mut pipeline);
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        for (file_index, segment_number) in lease_the_queue(&mut pipeline) {
            if file_index == missing_volume {
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
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
        witness.observe(&pipeline, job_id);
        if let Some(done) = next_owed_extraction(&mut pipeline, job_id).await {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
    }
    witness.observe(&pipeline, job_id);

    MissingVolumeRun {
        pipeline,
        _temp_dir: temp_dir,
        output_root,
        witness,
        volumes,
        members: vec![(MEMBER, member), (SECOND_MEMBER, second)],
        job_id,
    }
}

impl MissingVolumeRun {
    fn sets(&self) -> String {
        self.witness.seen.join("\n")
    }

    /// Every member the archive holds is on disk under the job's output root,
    /// byte for byte.
    fn assert_members_published(&self) {
        let sets = self.sets();
        for (name, expected) in &self.members {
            assert_eq!(
                std::fs::read(self.output_root.join(name)).ok().as_ref(),
                Some(expected),
                "{name} must be published whole\nsets: {sets}"
            );
        }
    }
}

/// A **whole** middle volume that never arrives is rebuilt by the recovery set
/// and routed back into the live direct set: the members are published from
/// the set's own partials, the set never demotes, and only the missing volume
/// is ever materialized — under a scratch name, not its own.
///
/// This is the hole a byte-split container is most exposed to: nothing in the
/// missing volume is a header, so the set has no fact about it beyond the
/// geometry volume 0 states and the tail's end header. Every byte of it is a
/// member extent that PAR2 alone can supply, and the repair must land those
/// bytes through the same layout the downloaded volumes were routed through.
#[tokio::test]
async fn sevenz_store_par2_rebuilds_a_wholly_missing_middle_volume() {
    let missing_volume = 1u32;
    let run = run_with_a_wholly_missing_volume(JobId(9_607), missing_volume).await;
    let sets = run.sets();
    assert_eq!(
        job_status_for_assert(&run.pipeline, run.job_id),
        Some(JobStatus::Complete),
        "sets: {sets}"
    );
    assert_eq!(
        run.pipeline.direct_store.finalized_sets, 1,
        "the set must commit from its own partials after the repair\nsets: {sets}"
    );
    assert!(
        !run.witness.contains("Demoted"),
        "a repairable hole must never demote the set\nsets: {sets}"
    );
    assert_eq!(
        run.pipeline.direct_store.repair_materialized_volumes, 1,
        "only the missing volume is materialized, as repair scratch\nsets: {sets}"
    );
    assert!(
        !run.output_root
            .join(&run.volumes[missing_volume as usize].0)
            .exists(),
        "the rebuilt volume is scratch, never published under its own name"
    );
    run.assert_members_published();
}

/// A **whole** tail volume that never arrives takes the end header with it, so
/// the set can never read its map and demotes — by design, not by accident.
/// What the demotion must then deliver is the ordinary path: the downloaded
/// volumes hand back to disk, the recovery set rebuilds the missing one there,
/// and extraction publishes every member whole. A missing file is repaired
/// either way; only *where* the repair lands differs.
#[tokio::test]
async fn sevenz_store_par2_rebuilds_a_wholly_missing_tail_volume_after_demotion() {
    let run = run_with_a_wholly_missing_volume(JobId(9_608), 2).await;
    let sets = run.sets();
    assert_eq!(
        job_status_for_assert(&run.pipeline, run.job_id),
        Some(JobStatus::Complete),
        "sets: {sets}"
    );
    assert!(
        run.witness.contains("Demoted"),
        "a set whose end header never arrives cannot stay direct\nsets: {sets}"
    );
    assert_eq!(
        run.pipeline.direct_store.finalized_sets, 0,
        "a demoted set publishes through extraction, not from partials\nsets: {sets}"
    );
    run.assert_members_published();
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
/// A split container is a byte split at a fixed part size, so its whole
/// geometry follows from two facts, and volume zero's front carries both: the
/// part size is that volume's own yEnc length, and the total is in the 32-byte
/// start header at its offset zero. Everything else — where each volume begins
/// in container coordinates, how long the short tail is — is arithmetic over
/// those two. The one thing they do not give is the map, which sits in the end
/// header at the very last byte of the set.
///
/// So the set reaches past its own retention limit for two articles and no
/// others: volume zero's front and the last volume's tail. Nothing is asked of
/// the volumes in between, and nothing they could say would be waited on — a
/// set that waited for every volume to declare a length would hold the whole
/// container as holds before a byte routed, which is the opposite of what
/// direct routing is for.
///
/// The arrangement below is what makes that visible. Volume zero's articles
/// arrive before the limit tightens and stage as holds, because there is no map
/// to route them against; from there nothing fits, and the only article the set
/// can be handed is the one it reaches past the limit for.
#[tokio::test]
async fn sevenz_store_probes_the_container_ends_rather_than_staging_it() {
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
    let mut witness = SetWitness::default();
    drive_sevenz(
        &mut pipeline,
        job_id,
        &working_dir,
        &mut peak_working_bytes,
        None,
        &mut witness,
    )
    .await;
    let sets = witness.render();
    assert!(
        peak_staged < capacity,
        "the set must never have held the container it could not hold\nsets: {sets}"
    );
    // Volume zero arrived before the limit tightened, so both geometry facts
    // were already in hand and its front was never probed for. What is left is
    // the far end of the set, and nothing in between: the middle volumes are
    // placed by arithmetic, so the parse spends no article on either of them.
    assert!(
        probed_volumes
            .iter()
            .all(|(volume, _)| *volume == VOLUMES as u32 - 1),
        "the parse must reach past the limit for the tail and for nothing else\nprobed: \
         {probed_volumes:?}\nsets: {sets}"
    );
    assert!(
        probed_volumes.contains(&(VOLUMES as u32 - 1, ARTICLES as u32 - 1)),
        "and the article it reaches for must be the one the end header is in\nprobed: \
         {probed_volumes:?}"
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
    let mut witness = SetWitness::default();
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
        witness.observe(&pipeline, job_id);
    }
    settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;
    let sets = witness.render();
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
    let mut witness = SetWitness::default();
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
        witness.observe(&pipeline, job_id);
    }
    settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;
    let sets = witness.render();
    assert!(
        sets.contains("EncryptedContent"),
        "the keyed header must be read, and its encrypted content reported\nsets: {sets}"
    );
    assert!(
        !sets.contains("EncryptedHeader"),
        "a container the job holds the password for must not refuse unread\nsets: {sets}"
    );
}

/// Drives a `-mhe` container to a verdict with the job's password picture set
/// by its spec and its persisted NZB, which is what the harvest reads.
async fn header_encrypted_verdict(
    job_id: JobId,
    archive_password: &str,
    spec_password: Option<&str>,
    nzb_zstd: Vec<u8>,
) -> String {
    let member = payload(59, 30_000);
    let archive = build_7z_shaped(
        &[Entry::file(MEMBER, member)],
        EncoderMethod::COPY,
        Some(archive_password),
        true,
    );
    let volumes = split_volumes(&archive, 2);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    spec.password = spec_password.map(str::to_owned);
    insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, nzb_zstd).await;
    let mut witness = SetWitness::default();
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
        witness.observe(&pipeline, job_id);
    }
    settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;
    witness.render()
}

/// The key is in the NZB's meta and the spec carries an operator's guess.
///
/// The spec's password is only the *first* of the job's candidates. Reading it
/// alone refuses a container for a password the job was holding all along —
/// which is the same reason the `-hp` gate is offered the whole harvest rather
/// than `spec.password`. Reaching the content refusal is the proof the meta
/// candidate was tried after the guess was refuted.
#[tokio::test]
async fn sevenz_store_opens_a_container_keyed_by_the_nzb_meta_password() {
    let sets = header_encrypted_verdict(
        JobId(9_612),
        "horizon",
        Some("not-the-password"),
        sample_nzb_zstd_with_password("horizon"),
    )
    .await;
    assert!(
        sets.contains("EncryptedContent"),
        "the meta candidate must be tried after the spec's guess\nsets: {sets}"
    );
    assert!(
        !sets.contains("EncryptedHeader"),
        "a container one of the job's candidates opens must not refuse\nsets: {sets}"
    );
}

/// No candidate the job holds opens the header.
///
/// `EncryptedHeader` is then the whole verdict, and it is stated once for the
/// list rather than per candidate — the same shape the `-hp` gate's
/// `NoVerifiedCandidate` has, and sticky for the same reason: the set demotes,
/// and the conventional extractor asks the same list again with nothing left
/// for this router to add.
#[tokio::test]
async fn sevenz_store_refuses_a_container_no_candidate_opens() {
    let sets = header_encrypted_verdict(
        JobId(9_613),
        "horizon",
        Some("not-the-password"),
        sample_nzb_zstd_with_password("also-wrong"),
    )
    .await;
    assert!(
        sets.contains("EncryptedHeader"),
        "every candidate refuted is the refusal\nsets: {sets}"
    );
    assert!(
        !sets.contains("EncryptedContent"),
        "a header nothing opened states nothing about its content\nsets: {sets}"
    );
}

/// Runs a three-volume container with one volume terminally unavailable.
///
/// Every article of the set leaves the queue, the way leasing one empties it;
/// the stranded volume's simply never come back — no result, no retry — which
/// is what an article nothing will ever deliver looks like from here.
///
/// Returns the set's state twice: with the rest of the container landed and
/// the stranded volume still owed, and again once nothing is owed at all.
async fn sevenz_set_with_a_stranded_volume(job_id: JobId, stranded: u32) -> (String, String) {
    const VOLUMES: usize = 3;
    let member = payload(61, 36_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, VOLUMES);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // One at a time, in dispatch order: an article leaves the queue when it is
    // leased and its result lands after that, so the set is only ever starved
    // of everything at the very end.
    let mut owed = SetWitness::default();
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index == stranded {
            continue;
        }
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
            &volumes,
            file_index,
            segment_number,
            ARTICLES_PER_VOLUME,
        )
        .await;
        owed.observe(&pipeline, job_id);
    }
    let while_owed = owed.render();
    // The stranded volume's articles are leased last and never come back, so
    // the set runs out of everything here rather than while the rest was still
    // arriving. The job advance that follows is the seam the verdict is taken
    // at, and in a running server every path to "nothing more is owed" reaches
    // it — an exhausted retry, a closing pass, a finished article.
    for segment_number in 0..ARTICLES_PER_VOLUME as u32 {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: stranded,
                },
                segment_number,
            },
        );
    }
    pipeline.check_job_completion(job_id).await;
    // Taken through a witness rather than off the live list: the verdict this
    // seam reaches retires the set that carries it, and which side of that the
    // read lands on is not something the test is entitled to assume.
    let mut starved = SetWitness::default();
    settle_sevenz_verdict(&mut pipeline, job_id, &mut starved).await;
    (while_owed, starved.render())
}

/// The map lives in the last volume's last article. With that volume
/// terminally unavailable there is nothing left that could ever read it.
///
/// Nothing else ends that wait. The probe planner skips a volume with nothing
/// left to ask for, so the gate stays shut with no request outstanding to
/// reopen it, and the holds ceilings only fire on a set big enough to reach
/// them — this one is far too small. Before the sweep this was a job parked at
/// its last article for good.
#[tokio::test]
async fn a_sevenz_set_whose_tail_never_arrives_demotes() {
    let (_, sets) = sevenz_set_with_a_stranded_volume(JobId(9_614), 2).await;
    assert!(
        sets.contains("Demoted") && sets.contains("UnreadableMap"),
        "a map no article will deliver must end the set's wait\nsets: {sets}"
    );
}

/// Volume zero carries both facts the geometry is derived from, so a set that
/// never receives it never places its map either — the same verdict, reached
/// from the other end of the container.
#[tokio::test]
async fn a_sevenz_set_whose_first_volume_never_arrives_demotes() {
    let (_, sets) = sevenz_set_with_a_stranded_volume(JobId(9_615), 0).await;
    assert!(
        sets.contains("Demoted") && sets.contains("UnreadableMap"),
        "a geometry no article will state must end the set's wait\nsets: {sets}"
    );
}

/// A volume in the middle is a different case, and the distinction is the
/// whole point of deriving the geometry from two facts instead of from every
/// volume's declared length: nothing in the middle is needed to read the map.
/// This set reads its map, routes what it has, and carries the shortfall
/// forward as the ordinary one it is — bytes that have not arrived — rather
/// than being parked waiting for a length it was never going to be told.
#[tokio::test]
async fn a_sevenz_set_missing_a_middle_volume_still_reads_its_map() {
    let (while_owed, starved) = sevenz_set_with_a_stranded_volume(JobId(9_616), 1).await;
    assert!(
        while_owed.contains("Routing") && !while_owed.contains("Demoted"),
        "the map reads with nothing heard from the middle\nsets: {while_owed}"
    );
    assert!(
        !starved.contains("UnreadableMap"),
        "and running out of articles is not a verdict on a map already read\nsets: {starved}"
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

/// The same container with its signature header pointing at an end header far
/// beyond anything that was posted.
fn overstate_next_header_offset(archive: &[u8]) -> Vec<u8> {
    let mut out = archive.to_vec();
    out[12..20].copy_from_slice(&(1u64 << 50).to_le_bytes());
    out
}

/// A start header is read before a single byte of it has been checked against
/// anything, so the part count it implies is arithmetic over two numbers a bad
/// posting is free to have made up. The ceiling is what keeps a container
/// claiming a petabyte from asking this router to plan a map for it.
///
/// The verdict lands on volume zero's very first article — the one that carries
/// both facts — so nothing of the set is ever held against the claim.
#[tokio::test]
async fn sevenz_store_refuses_a_start_header_claiming_more_parts_than_can_exist() {
    let member = payload(71, 30_000);
    let archive = overstate_next_header_offset(&build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    ));
    let volumes = split_volumes(&archive, 2);
    let job_id = JobId(9_617);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let mut witness = SetWitness::default();
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES_PER_VOLUME).await;
    settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;

    let sets = witness.render();
    assert!(
        witness.contains("Demoted"),
        "the claim must be refused on the article that makes it\nsets: {sets}"
    );
    assert!(
        sets.contains("VolumeSize"),
        "and refused as a geometry this router will not plan for\nsets: {sets}"
    );
}

/// Volume zero's yEnc length is a hint, not a fact this router may fail a job
/// over.
///
/// Weaver does not validate a posting's declared sizes anywhere else — the
/// `=yend` CRC is the acceptance test — and a container is no exception. The
/// hint is used to place the map; every disagreement with it costs the set its
/// route and nothing more, with the job left to the conventional path. Here
/// volume zero overstates itself by a byte, which the next volume's own
/// declaration contradicts.
#[tokio::test]
async fn sevenz_store_demotes_a_container_whose_part_size_hint_is_wrong() {
    let member = payload(73, 36_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let overstated = BTreeMap::from([(0, volumes[0].1.len() as u64 + 1)]);

    let outcome = run_sevenz_gate_awaiting(
        JobId(9_618),
        &volumes,
        &overstated,
        &in_order_arrivals(volumes.len()),
        &[MEMBER],
        Some("VolumeSize"),
    )
    .await;
    assert!(
        outcome.sets.contains("Demoted") && outcome.sets.contains("VolumeSize"),
        "a hint the posting contradicts must demote the set\nsets: {}",
        outcome.sets
    );
    assert!(
        !matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "and must cost the set its route rather than the job its download\nsets: {}",
        outcome.sets
    );
}

/// With no length stated for volume zero there is no part size, and with no
/// part size there is no map to place — this route needs the hint, and the
/// conventional path does not. So the set steps aside rather than guessing.
#[tokio::test]
async fn sevenz_store_refuses_a_container_whose_first_volume_states_no_length() {
    let member = payload(79, 36_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 2);
    let unstated = BTreeMap::from([(0, 0)]);

    let outcome = run_sevenz_gate_awaiting(
        JobId(9_619),
        &volumes,
        &unstated,
        &in_order_arrivals(volumes.len()),
        &[MEMBER],
        Some("VolumeHintUnusable"),
    )
    .await;
    assert!(
        outcome.sets.contains("VolumeHintUnusable"),
        "a container with no part size states nothing this route can use\nsets: {}",
        outcome.sets
    );
    assert!(
        !matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "and the conventional path must take it from there\nsets: {}",
        outcome.sets
    );
}

/// What a volume actually decoded to is the authority; the declaration was only
/// ever the early warning.
///
/// A volume that ends shorter than the map placed it means every byte routed
/// after its boundary went somewhere wrong, so the check is deliberately not
/// conditional on having routed nothing yet: the set demotes, and the members
/// it was part-way through writing are not shipped.
#[tokio::test]
async fn sevenz_store_demotes_a_volume_that_decodes_shorter_than_the_map_placed_it() {
    /// Bytes withheld from the middle volume's last article.
    const SHORT_BY: usize = 8;
    let member = payload(83, 36_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let volumes = split_volumes(&archive, 3);
    let job_id = JobId(9_620);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(&volumes, ARTICLES_PER_VOLUME);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut witness = SetWitness::default();
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (1, ARTICLES_PER_VOLUME as u32 - 1) {
            let (filename, bytes) = &volumes[1];
            let (start, end) = article_extent(bytes.len(), segment_number, ARTICLES_PER_VOLUME);
            submit_decoded_segment(
                &mut pipeline,
                NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number,
                start as u64,
                &bytes[start..end - SHORT_BY],
                filename,
                None,
            )
            .await;
            witness.observe(&pipeline, job_id);
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
        witness.observe(&pipeline, job_id);
    }
    settle_sevenz_verdict(&mut pipeline, job_id, &mut witness).await;

    let sets = witness.render();
    assert!(
        sets.contains("Demoted") && sets.contains("VolumeSize"),
        "a volume shorter than its place in the map must demote the set\nsets: {sets}"
    );
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert!(
        [output_root.join(MEMBER), working_dir.join(MEMBER)]
            .iter()
            .all(|path| !path.exists()),
        "and the member it was writing must not be shipped\nsets: {sets}"
    );
}
